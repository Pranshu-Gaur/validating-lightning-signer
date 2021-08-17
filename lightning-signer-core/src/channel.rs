use core::any::Any;
use core::fmt;
use core::fmt::{Debug, Error, Formatter};

use bitcoin::hashes::hex;
use bitcoin::hashes::sha256::Hash as Sha256Hash;
use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::{self, All, Message, PublicKey, Secp256k1, SecretKey, Signature};
use bitcoin::util::bip143::SigHashCache;
use bitcoin::{Network, OutPoint, Script, SigHashType};
use lightning::chain;
use lightning::chain::keysinterface::{BaseSign, InMemorySigner, KeysInterface};
use lightning::ln::chan_utils::{
    build_htlc_transaction, derive_private_key, get_htlc_redeemscript, make_funding_redeemscript,
    ChannelPublicKeys, ChannelTransactionParameters, CommitmentTransaction,
    CounterpartyChannelTransactionParameters, HTLCOutputInCommitment, HolderCommitmentTransaction,
    TxCreationKeys,
};
use log::{debug, trace};

use crate::node::Node;
use crate::policy::error::policy_error;
use crate::policy::validator::{EnforcementState, Validator, ValidatorState};
use crate::prelude::{Box, ToString, Vec};
use crate::tx::tx::{
    build_close_tx, build_commitment_tx, get_commitment_transaction_number_obscure_factor,
    sign_commitment, CommitmentInfo, CommitmentInfo2, HTLCInfo2,
};
use crate::util::crypto_utils::{
    derive_private_revocation_key, derive_public_key, derive_revocation_pubkey, payload_for_p2wpkh,
};
use crate::util::debug_utils::DebugHTLCOutputInCommitment;
use crate::util::status::{internal_error, invalid_argument, Status};
use crate::util::INITIAL_COMMITMENT_NUMBER;
use crate::{Arc, Weak};

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct ChannelId(pub [u8; 32]);

impl Debug for ChannelId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        hex::format_hex(&self.0, f)
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        hex::format_hex(&self.0, f)
    }
}

/// The commitment type, based on the negotiated option
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CommitmentType {
    Legacy,
    StaticRemoteKey,
    Anchors,
}

/// The negotiated parameters for the [Channel]
#[derive(Clone)]
pub struct ChannelSetup {
    /// Whether the channel is outbound
    pub is_outbound: bool,
    /// The total the channel was funded with
    pub channel_value_sat: u64,
    // DUP keys.inner.channel_value_satoshis
    /// How much was pushed to the counterparty
    pub push_value_msat: u64,
    /// The funding outpoint
    pub funding_outpoint: OutPoint,
    /// locally imposed requirement on the remote commitment transaction to_self_delay
    pub holder_selected_contest_delay: u16,
    /// Maybe be None if we should generate it inside the signer
    pub holder_shutdown_script: Option<Script>,
    /// The counterparty's basepoints and pubkeys
    pub counterparty_points: ChannelPublicKeys, // DUP keys.inner.remote_channel_pubkeys
    /// remotely imposed requirement on the local commitment transaction to_self_delay
    pub counterparty_selected_contest_delay: u16,
    /// The counterparty's shutdown script, for mutual close
    pub counterparty_shutdown_script: Script,
    /// The negotiated commitment type
    pub commitment_type: CommitmentType,
}

// Need to define manually because ChannelPublicKeys doesn't derive Debug.
impl fmt::Debug for ChannelSetup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelSetup")
            .field("is_outbound", &self.is_outbound)
            .field("channel_value_sat", &self.channel_value_sat)
            .field("push_value_msat", &self.push_value_msat)
            .field("funding_outpoint", &self.funding_outpoint)
            .field(
                "holder_selected_contest_delay",
                &self.holder_selected_contest_delay,
            )
            .field("holder_shutdown_script", &self.holder_shutdown_script)
            .field(
                "counterparty_points",
                log_channel_public_keys!(&self.counterparty_points),
            )
            .field(
                "counterparty_selected_contest_delay",
                &self.counterparty_selected_contest_delay,
            )
            .field(
                "counterparty_shutdown_script",
                &self.counterparty_shutdown_script,
            )
            .field("commitment_type", &self.commitment_type)
            .finish()
    }
}

impl ChannelSetup {
    pub(crate) fn option_static_remotekey(&self) -> bool {
        self.commitment_type != CommitmentType::Legacy
    }

    pub(crate) fn option_anchor_outputs(&self) -> bool {
        self.commitment_type == CommitmentType::Anchors
    }
}

/// A trait implemented by both channel states.  See [ChannelSlot]
pub trait ChannelBase: Any {
    /// Get the channel basepoints and public keys
    fn get_channel_basepoints(&self) -> ChannelPublicKeys;
    /// Get the per-commitment point for a holder commitment transaction
    fn get_per_commitment_point(&self, commitment_number: u64) -> Result<PublicKey, Status>;
    /// Get the per-commitment secret for a holder commitment transaction
    // TODO leaking secret
    fn get_per_commitment_secret(&self, commitment_number: u64) -> Result<SecretKey, Status>;
    /// Check a future secret to support `option_data_loss_protect`
    fn check_future_secret(&self, commit_num: u64, suggested: &SecretKey) -> Result<bool, Status>;
    /// Get the channel nonce, used to derive the channel keys
    // TODO should this be exposed?
    fn nonce(&self) -> Vec<u8>;

    // TODO remove when LDK workaround is removed in LoopbackSigner
    #[cfg(feature = "test_utils")]
    fn set_next_holder_commit_num_for_testing(&mut self, _num: u64) {
        // Do nothing for ChannelStub.  Channel will override.
    }
}

/// A channel can be in two states - before [Node::ready_channel] it's a
/// [ChannelStub], afterwards it's a [Channel].  This enum keeps track
/// of the two different states.
pub enum ChannelSlot {
    Stub(ChannelStub),
    Ready(Channel),
}

impl ChannelSlot {
    /// Get the channel nonce, used to derive the channel keys
    pub fn nonce(&self) -> Vec<u8> {
        match self {
            ChannelSlot::Stub(stub) => stub.nonce(),
            ChannelSlot::Ready(chan) => chan.nonce(),
        }
    }

    pub fn id(&self) -> ChannelId {
        match self {
            ChannelSlot::Stub(stub) => stub.id0,
            ChannelSlot::Ready(chan) => chan.id0,
        }
    }
}

/// A channel takes this form after [Node::new_channel], and before [Node::ready_channel]
#[derive(Clone)]
pub struct ChannelStub {
    /// A backpointer to the node
    pub node: Weak<Node>,
    /// The channel nonce, used to derive keys
    pub nonce: Vec<u8>,
    pub(crate) secp_ctx: Secp256k1<All>,
    /// The signer for this channel
    pub keys: InMemorySigner, // Incomplete, channel_value_sat is placeholder.
    /// The initial channel ID, used to find the channel in the node
    pub id0: ChannelId,
}

impl ChannelBase for ChannelStub {
    fn get_channel_basepoints(&self) -> ChannelPublicKeys {
        self.keys.pubkeys().clone()
    }

    fn get_per_commitment_point(&self, commitment_number: u64) -> Result<PublicKey, Status> {
        if commitment_number != 0 {
            return Err(policy_error(format!(
                "channel stub can only return point for commitment number zero",
            ))
            .into());
        }
        Ok(self.keys.get_per_commitment_point(
            INITIAL_COMMITMENT_NUMBER - commitment_number,
            &self.secp_ctx,
        ))
    }

    fn get_per_commitment_secret(&self, _commitment_number: u64) -> Result<SecretKey, Status> {
        // We can't release a commitment_secret from a ChannelStub ever.
        Err(policy_error(format!("channel stub cannot release commitment secret")).into())
    }

    fn check_future_secret(
        &self,
        commitment_number: u64,
        suggested: &SecretKey,
    ) -> Result<bool, Status> {
        let secret_data = self
            .keys
            .release_commitment_secret(INITIAL_COMMITMENT_NUMBER - commitment_number);
        Ok(suggested[..] == secret_data)
    }

    fn nonce(&self) -> Vec<u8> {
        self.nonce.clone()
    }
}

impl ChannelStub {
    pub(crate) fn channel_keys_with_channel_value(&self, channel_value_sat: u64) -> InMemorySigner {
        let secp_ctx = Secp256k1::signing_only();
        let keys = &self.keys;
        InMemorySigner::new(
            &secp_ctx,
            keys.funding_key,
            keys.revocation_base_key,
            keys.payment_key,
            keys.delayed_payment_base_key,
            keys.htlc_base_key,
            keys.commitment_seed,
            channel_value_sat,
            keys.channel_keys_id(),
        )
    }
}

/// After [Node::ready_channel]
#[derive(Clone)]
pub struct Channel {
    /// A backpointer to the node
    pub node: Weak<Node>,
    /// The channel nonce, used to derive keys
    pub nonce: Vec<u8>,
    /// The logger
    pub(crate) secp_ctx: Secp256k1<All>,
    /// The signer for this channel
    pub keys: InMemorySigner,
    // Channel state for policy enforcement purposes
    pub enforcement_state: EnforcementState,
    /// The negotiated channel setup
    pub setup: ChannelSetup,
    /// The initial channel ID
    pub id0: ChannelId,
    /// The optional permanent channel ID
    pub id: Option<ChannelId>,
}

impl Debug for Channel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("channel")
    }
}

impl ChannelBase for Channel {
    // TODO move out to impl Channel {} once LDK workaround is removed
    #[cfg(feature = "test_utils")]
    fn set_next_holder_commit_num_for_testing(&mut self, num: u64) {
        self.enforcement_state
            .set_next_holder_commit_num_for_testing(num);
    }

    fn get_channel_basepoints(&self) -> ChannelPublicKeys {
        self.keys.pubkeys().clone()
    }

    fn get_per_commitment_point(&self, commitment_number: u64) -> Result<PublicKey, Status> {
        let next_holder_commit_num = self.enforcement_state.next_holder_commit_num;
        if commitment_number > next_holder_commit_num {
            return Err(policy_error(format!(
                "get_per_commitment_point: \
                 commitment_number {} invalid when next_holder_commit_num is {}",
                commitment_number, next_holder_commit_num,
            ))
            .into());
        }
        Ok(self.keys.get_per_commitment_point(
            INITIAL_COMMITMENT_NUMBER - commitment_number,
            &self.secp_ctx,
        ))
    }

    fn get_per_commitment_secret(&self, commitment_number: u64) -> Result<SecretKey, Status> {
        let next_holder_commit_num = self.enforcement_state.next_holder_commit_num;
        // policy-v2-revoke-new-commitment-signed
        if commitment_number + 2 > next_holder_commit_num {
            return Err(policy_error(format!(
                "get_per_commitment_secret: \
                 commitment_number {} invalid when next_holder_commit_num is {}",
                commitment_number, next_holder_commit_num,
            ))
            .into());
        }
        let secret = self
            .keys
            .release_commitment_secret(INITIAL_COMMITMENT_NUMBER - commitment_number);
        Ok(SecretKey::from_slice(&secret).unwrap())
    }

    fn check_future_secret(
        &self,
        commitment_number: u64,
        suggested: &SecretKey,
    ) -> Result<bool, Status> {
        let secret_data = self
            .keys
            .release_commitment_secret(INITIAL_COMMITMENT_NUMBER - commitment_number);
        Ok(suggested[..] == secret_data)
    }

    fn nonce(&self) -> Vec<u8> {
        self.nonce.clone()
    }
}

impl Channel {
    #[cfg(feature = "test_utils")]
    pub fn set_next_counterparty_commit_num_for_testing(
        &mut self,
        num: u64,
        current_point: PublicKey,
    ) {
        self.enforcement_state
            .set_next_counterparty_commit_num_for_testing(num, current_point);
    }

    #[cfg(feature = "test_utils")]
    pub fn set_next_counterparty_revoke_num_for_testing(&mut self, num: u64) {
        self.enforcement_state
            .set_next_counterparty_revoke_num_for_testing(num);
    }
}

// Phase 2
impl Channel {
    // Phase 2
    pub(crate) fn make_counterparty_tx_keys(
        &self,
        per_commitment_point: &PublicKey,
    ) -> Result<TxCreationKeys, Status> {
        let holder_points = self.keys.pubkeys();

        let counterparty_points = self.keys.counterparty_pubkeys();

        Ok(self.make_tx_keys(per_commitment_point, counterparty_points, holder_points))
    }

    pub(crate) fn make_holder_tx_keys(
        &self,
        per_commitment_point: &PublicKey,
    ) -> Result<TxCreationKeys, Status> {
        let holder_points = self.keys.pubkeys();

        let counterparty_points = self.keys.counterparty_pubkeys();

        Ok(self.make_tx_keys(per_commitment_point, holder_points, counterparty_points))
    }

    fn make_tx_keys(
        &self,
        per_commitment_point: &PublicKey,
        a_points: &ChannelPublicKeys,
        b_points: &ChannelPublicKeys,
    ) -> TxCreationKeys {
        TxCreationKeys::derive_new(
            &self.secp_ctx,
            &per_commitment_point,
            &a_points.delayed_payment_basepoint,
            &a_points.htlc_basepoint,
            &b_points.revocation_basepoint,
            &b_points.htlc_basepoint,
        )
        .expect("failed to derive keys")
    }

    fn derive_counterparty_payment_pubkey(
        &self,
        remote_per_commitment_point: &PublicKey,
    ) -> Result<PublicKey, Status> {
        let holder_points = self.keys.pubkeys();
        let counterparty_key = if self.setup.option_static_remotekey() {
            holder_points.payment_point
        } else {
            derive_public_key(
                &self.secp_ctx,
                &remote_per_commitment_point,
                &holder_points.payment_point,
            )
            .map_err(|err| internal_error(format!("could not derive counterparty_key: {}", err)))?
        };
        Ok(counterparty_key)
    }

    fn get_commitment_transaction_number_obscure_factor(&self) -> u64 {
        get_commitment_transaction_number_obscure_factor(
            &self.keys.pubkeys().payment_point,
            &self.keys.counterparty_pubkeys().payment_point,
            self.setup.is_outbound,
        )
    }

    // forward counting commitment number
    #[allow(dead_code)]
    pub(crate) fn build_commitment_tx(
        &self,
        per_commitment_point: &PublicKey,
        commitment_number: u64,
        info: &CommitmentInfo2,
    ) -> Result<
        (
            bitcoin::Transaction,
            Vec<Script>,
            Vec<HTLCOutputInCommitment>,
        ),
        Status,
    > {
        let keys = if !info.is_counterparty_broadcaster {
            self.make_holder_tx_keys(per_commitment_point)?
        } else {
            self.make_counterparty_tx_keys(per_commitment_point)?
        };

        // TODO - consider if we can get LDK to put funding pubkeys in TxCreationKeys
        let (workaround_local_funding_pubkey, workaround_remote_funding_pubkey) =
            if !info.is_counterparty_broadcaster {
                (
                    &self.keys.pubkeys().funding_pubkey,
                    &self.keys.counterparty_pubkeys().funding_pubkey,
                )
            } else {
                (
                    &self.keys.counterparty_pubkeys().funding_pubkey,
                    &self.keys.pubkeys().funding_pubkey,
                )
            };

        let obscured_commitment_transaction_number =
            self.get_commitment_transaction_number_obscure_factor() ^ commitment_number;
        Ok(build_commitment_tx(
            &keys,
            info,
            obscured_commitment_transaction_number,
            self.setup.funding_outpoint,
            self.setup.option_anchor_outputs(),
            workaround_local_funding_pubkey,
            workaround_remote_funding_pubkey,
        ))
    }

    /// Sign a counterparty commitment transaction after rebuilding it
    /// from the supplied arguments.
    // TODO anchors support once LDK supports it
    pub fn sign_counterparty_commitment_tx_phase2(
        &self,
        remote_per_commitment_point: &PublicKey,
        commitment_number: u64,
        feerate_per_kw: u32,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<(Vec<u8>, Vec<Vec<u8>>), Status> {
        let htlcs = Self::htlcs_info2_to_oic(offered_htlcs, received_htlcs);

        let commitment_tx = self.make_counterparty_commitment_tx(
            remote_per_commitment_point,
            commitment_number,
            feerate_per_kw,
            to_holder_value_sat,
            to_counterparty_value_sat,
            htlcs,
        );

        debug!(
            "channel: sign counterparty txid {}",
            commitment_tx.trust().built_transaction().txid
        );

        let sigs = self
            .keys
            .sign_counterparty_commitment(&commitment_tx, &self.secp_ctx)
            .map_err(|_| internal_error("failed to sign"))?;
        let mut sig = sigs.0.serialize_der().to_vec();
        sig.push(SigHashType::All as u8);
        let mut htlc_sigs = Vec::new();
        for htlc_signature in sigs.1 {
            let mut htlc_sig = htlc_signature.serialize_der().to_vec();
            htlc_sig.push(SigHashType::All as u8);
            htlc_sigs.push(htlc_sig);
        }
        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;
        Ok((sig, htlc_sigs))
    }

    // This function is needed for testing with mutated keys.
    pub(crate) fn make_counterparty_commitment_tx_with_keys(
        &self,
        keys: TxCreationKeys,
        commitment_number: u64,
        feerate_per_kw: u32,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        htlcs: Vec<HTLCOutputInCommitment>,
    ) -> CommitmentTransaction {
        let mut htlcs_with_aux = htlcs.iter().map(|h| (h.clone(), ())).collect();
        let channel_parameters = self.make_channel_parameters();
        let parameters = channel_parameters.as_counterparty_broadcastable();
        let commitment_tx = CommitmentTransaction::new_with_auxiliary_htlc_data(
            INITIAL_COMMITMENT_NUMBER - commitment_number,
            to_counterparty_value_sat,
            to_holder_value_sat,
            keys,
            feerate_per_kw,
            &mut htlcs_with_aux,
            &parameters,
        );
        commitment_tx
    }

    pub(crate) fn make_counterparty_commitment_tx(
        &self,
        remote_per_commitment_point: &PublicKey,
        commitment_number: u64,
        feerate_per_kw: u32,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        htlcs: Vec<HTLCOutputInCommitment>,
    ) -> CommitmentTransaction {
        let keys = self
            .make_counterparty_tx_keys(remote_per_commitment_point)
            .unwrap();
        self.make_counterparty_commitment_tx_with_keys(
            keys,
            commitment_number,
            feerate_per_kw,
            to_holder_value_sat,
            to_counterparty_value_sat,
            htlcs,
        )
    }

    /// Sign a holder commitment transaction after rebuilding it
    /// from the supplied arguments.
    // TODO anchors support once upstream supports it
    pub fn sign_holder_commitment_tx_phase2(
        &self,
        commitment_number: u64,
        feerate_per_kw: u32,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<(Vec<u8>, Vec<Vec<u8>>), Status> {
        let htlcs = Self::htlcs_info2_to_oic(offered_htlcs, received_htlcs);

        // We provide a dummy signature for the remote, since we don't require that sig
        // to be passed in to this call.  It would have been better if HolderCommitmentTransaction
        // didn't require the remote sig.
        // TODO consider if we actually want the sig for policy checks
        let dummy_sig = Secp256k1::new().sign(
            &secp256k1::Message::from_slice(&[42; 32]).unwrap(),
            &SecretKey::from_slice(&[42; 32]).unwrap(),
        );
        let mut htlc_dummy_sigs = Vec::with_capacity(htlcs.len());
        htlc_dummy_sigs.resize(htlcs.len(), dummy_sig);

        let commitment_tx = self.make_holder_commitment_tx(
            commitment_number,
            feerate_per_kw,
            to_holder_value_sat,
            to_counterparty_value_sat,
            htlcs,
        )?;
        debug!(
            "channel: sign holder txid {}",
            commitment_tx.trust().built_transaction().txid
        );

        let holder_commitment_tx = HolderCommitmentTransaction::new(
            commitment_tx,
            dummy_sig,
            htlc_dummy_sigs,
            &self.keys.pubkeys().funding_pubkey,
            &self.keys.counterparty_pubkeys().funding_pubkey,
        );

        let (sig, htlc_sigs) = self
            .keys
            .sign_holder_commitment_and_htlcs(&holder_commitment_tx, &self.secp_ctx)
            .map_err(|_| internal_error("failed to sign"))?;
        let mut sig_vec = sig.serialize_der().to_vec();
        sig_vec.push(SigHashType::All as u8);

        let mut htlc_sig_vecs = Vec::new();
        for htlc_sig in htlc_sigs {
            let mut htlc_sig_vec = htlc_sig.serialize_der().to_vec();
            htlc_sig_vec.push(SigHashType::All as u8);
            htlc_sig_vecs.push(htlc_sig_vec);
        }
        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;
        Ok((sig_vec, htlc_sig_vecs))
    }

    // This function is needed for testing with mutated keys.
    pub(crate) fn make_holder_commitment_tx_with_keys(
        &self,
        keys: TxCreationKeys,
        commitment_number: u64,
        feerate_per_kw: u32,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        htlcs: Vec<HTLCOutputInCommitment>,
    ) -> CommitmentTransaction {
        let mut htlcs_with_aux = htlcs.iter().map(|h| (h.clone(), ())).collect();
        let channel_parameters = self.make_channel_parameters();
        let parameters = channel_parameters.as_holder_broadcastable();
        let commitment_tx = CommitmentTransaction::new_with_auxiliary_htlc_data(
            INITIAL_COMMITMENT_NUMBER - commitment_number,
            to_holder_value_sat,
            to_counterparty_value_sat,
            keys,
            feerate_per_kw,
            &mut htlcs_with_aux,
            &parameters,
        );
        commitment_tx
    }

    pub(crate) fn make_holder_commitment_tx(
        &self,
        commitment_number: u64,
        feerate_per_kw: u32,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        htlcs: Vec<HTLCOutputInCommitment>,
    ) -> Result<CommitmentTransaction, Status> {
        let per_commitment_point = self.get_per_commitment_point(commitment_number)?;
        let keys = self.make_holder_tx_keys(&per_commitment_point).unwrap();
        Ok(self.make_holder_commitment_tx_with_keys(
            keys,
            commitment_number,
            feerate_per_kw,
            to_holder_value_sat,
            to_counterparty_value_sat,
            htlcs,
        ))
    }

    pub(crate) fn htlcs_info2_to_oic(
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Vec<HTLCOutputInCommitment> {
        let mut htlcs = Vec::new();
        for htlc in offered_htlcs {
            htlcs.push(HTLCOutputInCommitment {
                offered: true,
                amount_msat: htlc.value_sat * 1000,
                cltv_expiry: htlc.cltv_expiry,
                payment_hash: htlc.payment_hash,
                transaction_output_index: None,
            });
        }
        for htlc in received_htlcs {
            htlcs.push(HTLCOutputInCommitment {
                offered: false,
                amount_msat: htlc.value_sat * 1000,
                cltv_expiry: htlc.cltv_expiry,
                payment_hash: htlc.payment_hash,
                transaction_output_index: None,
            });
        }
        htlcs
    }

    /// Build channel parameters, used to further build a commitment transaction
    pub fn make_channel_parameters(&self) -> ChannelTransactionParameters {
        let funding_outpoint = chain::transaction::OutPoint {
            txid: self.setup.funding_outpoint.txid,
            index: self.setup.funding_outpoint.vout as u16,
        };
        let channel_parameters = ChannelTransactionParameters {
            holder_pubkeys: self.get_channel_basepoints(),
            holder_selected_contest_delay: self.setup.holder_selected_contest_delay,
            is_outbound_from_holder: self.setup.is_outbound,
            counterparty_parameters: Some(CounterpartyChannelTransactionParameters {
                pubkeys: self.setup.counterparty_points.clone(),
                selected_contest_delay: self.setup.counterparty_selected_contest_delay,
            }),
            funding_outpoint: Some(funding_outpoint),
        };
        channel_parameters
    }

    /// Get the shutdown script where our funds will go when we mutual-close
    pub fn get_shutdown_script(&self) -> Script {
        self.setup
            .holder_shutdown_script
            .clone()
            .unwrap_or_else(|| {
                payload_for_p2wpkh(&self.get_node().keys_manager.get_shutdown_pubkey())
                    .script_pubkey()
            })
    }

    fn get_node(&self) -> Arc<Node> {
        self.node.upgrade().unwrap()
    }

    /// Sign a mutual close transaction after rebuilding it from the supplied arguments
    pub fn sign_mutual_close_tx_phase2(
        &self,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        counterparty_shutdown_script: Option<Script>,
    ) -> Result<Signature, Status> {
        let holder_script = self.get_shutdown_script();

        let counterparty_script = counterparty_shutdown_script
            .as_ref()
            .unwrap_or(&self.setup.counterparty_shutdown_script);

        let tx = build_close_tx(
            to_holder_value_sat,
            to_counterparty_value_sat,
            &holder_script,
            counterparty_script,
            self.setup.funding_outpoint,
        );

        let res = self
            .keys
            .sign_closing_transaction(&tx, &self.secp_ctx)
            .map_err(|_| Status::internal("failed to sign"));
        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;
        res
    }

    /// Sign a delayed output that goes to us while sweeping a transaction we broadcast
    pub fn sign_delayed_sweep(
        &self,
        tx: &bitcoin::Transaction,
        input: usize,
        commitment_number: u64,
        redeemscript: &Script,
        htlc_amount_sat: u64,
    ) -> Result<Signature, Status> {
        let per_commitment_point = self.get_per_commitment_point(commitment_number)?;

        let htlc_sighash = Message::from_slice(
            &SigHashCache::new(tx).signature_hash(
                input,
                &redeemscript,
                htlc_amount_sat,
                SigHashType::All,
            )[..],
        )
        .map_err(|_| Status::internal("failed to sighash"))?;

        let htlc_privkey = derive_private_key(
            &self.secp_ctx,
            &per_commitment_point,
            &self.keys.delayed_payment_base_key,
        )
        .map_err(|_| Status::internal("failed to derive key"))?;

        let sig = self.secp_ctx.sign(&htlc_sighash, &htlc_privkey);
        self.persist()?;
        Ok(sig)
    }

    /// Sign TODO
    pub fn sign_counterparty_htlc_sweep(
        &self,
        tx: &bitcoin::Transaction,
        input: usize,
        remote_per_commitment_point: &PublicKey,
        redeemscript: &Script,
        htlc_amount_sat: u64,
    ) -> Result<Signature, Status> {
        let htlc_sighash = Message::from_slice(
            &SigHashCache::new(tx).signature_hash(
                input,
                &redeemscript,
                htlc_amount_sat,
                SigHashType::All,
            )[..],
        )
        .map_err(|_| Status::internal("failed to sighash"))?;

        let htlc_privkey = derive_private_key(
            &self.secp_ctx,
            &remote_per_commitment_point,
            &self.keys.htlc_base_key,
        )
        .map_err(|_| Status::internal("failed to derive key"))?;

        let sig = self.secp_ctx.sign(&htlc_sighash, &htlc_privkey);
        self.persist()?;
        Ok(sig)
    }

    /// Sign a justice transaction on an old state that the counterparty broadcast
    pub fn sign_justice_sweep(
        &self,
        tx: &bitcoin::Transaction,
        input: usize,
        revocation_secret: &SecretKey,
        redeemscript: &Script,
        htlc_amount_sat: u64,
    ) -> Result<Signature, Status> {
        let sighash = Message::from_slice(
            &SigHashCache::new(tx).signature_hash(
                input,
                &redeemscript,
                htlc_amount_sat,
                SigHashType::All,
            )[..],
        )
        .map_err(|_| Status::internal("failed to sighash"))?;

        let privkey = derive_private_revocation_key(
            &self.secp_ctx,
            revocation_secret,
            &self.keys.revocation_base_key,
        )
        .map_err(|_| Status::internal("failed to derive key"))?;

        let sig = self.secp_ctx.sign(&sighash, &privkey);
        self.persist()?;
        Ok(sig)
    }

    /// Sign a channel announcement with both the node key and the funding key
    pub fn sign_channel_announcement(&self, announcement: &Vec<u8>) -> (Signature, Signature) {
        let ann_hash = Sha256dHash::hash(announcement);
        let encmsg = secp256k1::Message::from_slice(&ann_hash[..]).expect("encmsg failed");

        (
            self.secp_ctx
                .sign(&encmsg, &self.get_node().get_node_secret()),
            self.secp_ctx.sign(&encmsg, &self.keys.funding_key),
        )
    }

    fn persist(&self) -> Result<(), Status> {
        let node_id = self.get_node().get_id();
        self.get_node()
            .persister
            .update_channel(&node_id, &self)
            .map_err(|_| Status::internal("persist failed"))
    }

    pub fn network(&self) -> Network {
        self.get_node().network
    }
}

// Phase 1
impl Channel {
    pub(crate) fn build_counterparty_commitment_info(
        &self,
        remote_per_commitment_point: &PublicKey,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<CommitmentInfo2, Status> {
        let holder_points = self.keys.pubkeys();
        let secp_ctx = &self.secp_ctx;

        let to_counterparty_delayed_pubkey = derive_public_key(
            secp_ctx,
            &remote_per_commitment_point,
            &self.setup.counterparty_points.delayed_payment_basepoint,
        )
        .map_err(|err| {
            internal_error(format!("could not derive to_holder_delayed_key: {}", err))
        })?;
        let counterparty_payment_pubkey =
            self.derive_counterparty_payment_pubkey(remote_per_commitment_point)?;
        let revocation_pubkey = derive_revocation_pubkey(
            secp_ctx,
            &remote_per_commitment_point,
            &holder_points.revocation_basepoint,
        )
        .map_err(|err| internal_error(format!("could not derive revocation key: {}", err)))?;
        let to_holder_pubkey = counterparty_payment_pubkey.clone();
        Ok(CommitmentInfo2 {
            is_counterparty_broadcaster: true,
            to_countersigner_pubkey: to_holder_pubkey,
            to_countersigner_value_sat: to_holder_value_sat,
            revocation_pubkey,
            to_broadcaster_delayed_pubkey: to_counterparty_delayed_pubkey,
            to_broadcaster_value_sat: to_counterparty_value_sat,
            to_self_delay: self.setup.holder_selected_contest_delay,
            offered_htlcs,
            received_htlcs,
        })
    }

    // TODO dead code
    #[allow(dead_code)]
    pub fn build_holder_commitment_info(
        &self,
        per_commitment_point: &PublicKey,
        to_holder_value_sat: u64,
        to_counterparty_value_sat: u64,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<CommitmentInfo2, Status> {
        let holder_points = self.keys.pubkeys();
        let counterparty_points = self.keys.counterparty_pubkeys();
        let secp_ctx = &self.secp_ctx;

        let to_holder_delayed_pubkey = derive_public_key(
            secp_ctx,
            &per_commitment_point,
            &holder_points.delayed_payment_basepoint,
        )
        .map_err(|err| {
            internal_error(format!(
                "could not derive to_holder_delayed_pubkey: {}",
                err
            ))
        })?;

        let counterparty_pubkey = if self.setup.option_static_remotekey() {
            counterparty_points.payment_point
        } else {
            derive_public_key(
                &self.secp_ctx,
                &per_commitment_point,
                &counterparty_points.payment_point,
            )
            .map_err(|err| {
                internal_error(format!("could not derive counterparty_pubkey: {}", err))
            })?
        };

        let revocation_pubkey = derive_revocation_pubkey(
            secp_ctx,
            &per_commitment_point,
            &counterparty_points.revocation_basepoint,
        )
        .map_err(|err| internal_error(format!("could not derive revocation_pubkey: {}", err)))?;
        let to_counterparty_pubkey = counterparty_pubkey.clone();
        Ok(CommitmentInfo2 {
            is_counterparty_broadcaster: false,
            to_countersigner_pubkey: to_counterparty_pubkey,
            to_countersigner_value_sat: to_counterparty_value_sat,
            revocation_pubkey,
            to_broadcaster_delayed_pubkey: to_holder_delayed_pubkey,
            to_broadcaster_value_sat: to_holder_value_sat,
            to_self_delay: self.setup.counterparty_selected_contest_delay,
            offered_htlcs,
            received_htlcs,
        })
    }

    /// Phase 1
    pub fn sign_counterparty_commitment_tx(
        &mut self,
        tx: &bitcoin::Transaction,
        output_witscripts: &Vec<Vec<u8>>,
        remote_per_commitment_point: &PublicKey,
        commitment_number: u64,
        feerate_per_kw: u32,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<Signature, Status> {
        if tx.output.len() != output_witscripts.len() {
            return Err(invalid_argument("len(tx.output) != len(witscripts)"));
        }

        let validator = self
            .node
            .upgrade()
            .unwrap()
            .validator_factory
            .make_validator(self.network());

        // Since we didn't have the value at the real open, validate it now.
        validator.validate_channel_open(&self.setup)?;

        // Derive a CommitmentInfo first, convert to CommitmentInfo2 below ...
        let is_counterparty = true;
        let info = validator.make_info(
            &self.keys,
            &self.setup,
            is_counterparty,
            tx,
            output_witscripts,
        )?;

        let info2 = self.build_counterparty_commitment_info(
            remote_per_commitment_point,
            info.to_countersigner_value_sat,
            info.to_broadcaster_value_sat,
            offered_htlcs,
            received_htlcs,
        )?;

        // TODO(devrandom) - obtain current_height so that we can validate the HTLC CLTV
        let vstate = ValidatorState { current_height: 0 };
        validator
            .validate_commitment_tx(
                &self.enforcement_state,
                commitment_number,
                &remote_per_commitment_point,
                &self.setup,
                &vstate,
                &info2,
            )
            .map_err(|ve| {
                debug!(
                    "VALIDATION FAILED: {}\ntx={:#?}\nsetup={:#?}\nvstate={:#?}\ninfo={:#?}",
                    ve, &tx, &self.setup, &vstate, &info2,
                );
                ve
            })?;

        let htlcs =
            Self::htlcs_info2_to_oic(info2.offered_htlcs.clone(), info2.received_htlcs.clone());

        let recomposed_tx = self.make_counterparty_commitment_tx(
            remote_per_commitment_point,
            commitment_number,
            feerate_per_kw,
            info.to_countersigner_value_sat,
            info.to_broadcaster_value_sat,
            htlcs,
        );

        if recomposed_tx.trust().built_transaction().transaction != *tx {
            debug!("ORIGINAL_TX={:#?}", &tx);
            debug!(
                "RECOMPOSED_TX={:#?}",
                &recomposed_tx.trust().built_transaction().transaction
            );
            return Err(policy_error("recomposed tx mismatch".to_string()).into());
        }

        // The comparison in the previous block will fail if any of the
        // following policies are violated:
        // - policy-v1-commitment-version
        // - policy-v1-commitment-locktime
        // - policy-v1-commitment-nsequence
        // - policy-v1-commitment-input-single
        // - policy-v1-commitment-input-match-funding
        // - policy-v1-commitment-revocation-pubkey
        // - policy-v1-commitment-htlc-pubkey
        // - policy-v1-commitment-delayed-pubkey

        // Convert from backwards counting.
        let commit_num = INITIAL_COMMITMENT_NUMBER - recomposed_tx.trust().commitment_number();

        let point = recomposed_tx.trust().keys().per_commitment_point;

        self.enforcement_state
            .set_next_counterparty_commit_num(commit_num + 1, point, info2)?;

        // Sign the recomposed commitment.
        let sigs = self
            .keys
            .sign_counterparty_commitment(&recomposed_tx, &self.secp_ctx)
            .map_err(|_| internal_error(format!("sign_counterparty_commitment failed")))?;

        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;

        // Discard the htlc signatures for now.
        Ok(sigs.0)
    }

    fn make_recomposed_holder_commitment_tx(
        &self,
        tx: &bitcoin::Transaction,
        output_witscripts: &Vec<Vec<u8>>,
        commitment_number: u64,
        feerate_per_kw: u32,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<(CommitmentTransaction, CommitmentInfo2), Status> {
        if tx.output.len() != output_witscripts.len() {
            return Err(invalid_argument(format!(
                "len(tx.output):{} != len(witscripts):{}",
                tx.output.len(),
                output_witscripts.len()
            )));
        }

        let validator = self
            .node
            .upgrade()
            .unwrap()
            .validator_factory
            .make_validator(self.network());

        // Since we didn't have the value at the real open, validate it now.
        validator.validate_channel_open(&self.setup)?;

        // Derive a CommitmentInfo first, convert to CommitmentInfo2 below ...
        let is_counterparty = false;
        let info = validator.make_info(
            &self.keys,
            &self.setup,
            is_counterparty,
            tx,
            output_witscripts,
        )?;

        self.make_recomposed_holder_commitment_tx_common(
            tx,
            commitment_number,
            feerate_per_kw,
            offered_htlcs,
            received_htlcs,
            validator,
            info,
        )
    }

    fn make_recomposed_holder_commitment_tx_common(
        &self,
        tx: &bitcoin::Transaction,
        commitment_number: u64,
        feerate_per_kw: u32,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
        validator: Box<dyn Validator>,
        info: CommitmentInfo,
    ) -> Result<(CommitmentTransaction, CommitmentInfo2), Status> {
        let commitment_point = &self.get_per_commitment_point(commitment_number)?;
        let info2 = self.build_holder_commitment_info(
            &commitment_point,
            info.to_broadcaster_value_sat,
            info.to_countersigner_value_sat,
            offered_htlcs,
            received_htlcs,
        )?;

        // TODO(devrandom) - obtain current_height so that we can validate the HTLC CLTV
        let state = ValidatorState { current_height: 0 };
        validator
            .validate_commitment_tx(
                &self.enforcement_state,
                commitment_number,
                commitment_point,
                &self.setup,
                &state,
                &info2,
            )
            .map_err(|ve| {
                debug!(
                    "VALIDATION FAILED: {}\ntx={:#?}\nsetup={:#?}\nstate={:#?}\ninfo={:#?}",
                    ve, &tx, &self.setup, &state, &info2,
                );
                ve
            })?;

        let htlcs =
            Self::htlcs_info2_to_oic(info2.offered_htlcs.clone(), info2.received_htlcs.clone());

        let recomposed_tx = self.make_holder_commitment_tx(
            commitment_number,
            feerate_per_kw,
            info.to_broadcaster_value_sat,
            info.to_countersigner_value_sat,
            htlcs.clone(),
        )?;

        if recomposed_tx.trust().built_transaction().transaction != *tx {
            debug!("ORIGINAL_TX={:#?}", &tx);
            debug!(
                "RECOMPOSED_TX={:#?}",
                &recomposed_tx.trust().built_transaction().transaction
            );
            return Err(policy_error("recomposed tx mismatch".to_string()).into());
        }

        // The comparison in the previous block will fail if any of the
        // following policies are violated:
        // - policy-v1-commitment-version
        // - policy-v1-commitment-locktime
        // - policy-v1-commitment-nsequence
        // - policy-v1-commitment-input-single
        // - policy-v1-commitment-input-match-funding
        // - policy-v1-commitment-revocation-pubkey
        // - policy-v1-commitment-htlc-pubkey
        // - policy-v1-commitment-delayed-pubkey
        // - policy-v2-revoke-new-commitment-valid

        Ok((recomposed_tx, info2))
    }

    /// Validate the counterparty's signatures on the holder's
    /// commitment and HTLCs when the commitment_signed message is
    /// received.  Returns the next per_commitment_point and the
    /// holder's revocation secret for the prior commitment.  This
    /// method advances the expected next holder commitment number in
    /// the signer's state.
    pub fn validate_holder_commitment_tx(
        &mut self,
        tx: &bitcoin::Transaction,
        output_witscripts: &Vec<Vec<u8>>,
        commitment_number: u64,
        feerate_per_kw: u32,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
        counterparty_commit_sig: &Signature,
        counterparty_htlc_sigs: &Vec<Signature>,
    ) -> Result<(PublicKey, Option<SecretKey>), Status> {
        let validator = self
            .node
            .upgrade()
            .unwrap()
            .validator_factory
            .make_validator(self.network());

        validator.validate_holder_commitment_state(&self.enforcement_state)?;

        let (recomposed_tx, info2) = self.make_recomposed_holder_commitment_tx(
            tx,
            output_witscripts,
            commitment_number,
            feerate_per_kw,
            offered_htlcs,
            received_htlcs,
        )?;

        let redeemscript = make_funding_redeemscript(
            &self.keys.pubkeys().funding_pubkey,
            &self.setup.counterparty_points.funding_pubkey,
        );

        let sig_hash_type = if self.setup.option_anchor_outputs() {
            SigHashType::SinglePlusAnyoneCanPay
        } else {
            SigHashType::All
        };

        let sighash = Message::from_slice(
            &SigHashCache::new(&recomposed_tx.trust().built_transaction().transaction)
                .signature_hash(
                    0,
                    &redeemscript,
                    self.setup.channel_value_sat,
                    sig_hash_type,
                )[..],
        )
        .map_err(|ve| internal_error(format!("sighash failed: {}", ve)))?;

        let secp_ctx = Secp256k1::new();
        secp_ctx
            .verify(
                &sighash,
                &counterparty_commit_sig,
                &self.setup.counterparty_points.funding_pubkey,
            )
            .map_err(|ve| policy_error(format!("commit sig verify failed: {}", ve)))?;

        let per_commitment_point = self.get_per_commitment_point(commitment_number)?;
        let txkeys = self
            .make_holder_tx_keys(&per_commitment_point)
            .map_err(|err| internal_error(format!("make_holder_tx_keys failed: {}", err)))?;
        let commitment_txid = recomposed_tx.trust().txid();
        let to_self_delay = self.setup.counterparty_selected_contest_delay;

        let htlc_pubkey = derive_public_key(
            &secp_ctx,
            &per_commitment_point,
            &self.keys.counterparty_pubkeys().htlc_basepoint,
        )
        .map_err(|err| internal_error(format!("derive_public_key failed: {}", err)))?;

        for ndx in 0..recomposed_tx.htlcs().len() {
            let htlc = &recomposed_tx.htlcs()[ndx];

            let htlc_redeemscript = get_htlc_redeemscript(htlc, &txkeys);

            let recomposed_htlc_tx = build_htlc_transaction(
                &commitment_txid,
                feerate_per_kw,
                to_self_delay,
                htlc,
                &txkeys.broadcaster_delayed_payment_key,
                &txkeys.revocation_key,
            );

            let recomposed_tx_sighash = Message::from_slice(
                &SigHashCache::new(&recomposed_htlc_tx).signature_hash(
                    0,
                    &htlc_redeemscript,
                    htlc.amount_msat / 1000,
                    SigHashType::All,
                )[..],
            )
            .map_err(|err| invalid_argument(format!("sighash failed for htlc {}: {}", ndx, err)))?;

            secp_ctx
                .verify(
                    &recomposed_tx_sighash,
                    &counterparty_htlc_sigs[ndx],
                    &htlc_pubkey,
                )
                .map_err(|err| {
                    policy_error(format!(
                        "commit sig verify failed for htlc {}: {}",
                        ndx, err
                    ))
                })?;
        }

        // Advance the local commitment number state.
        self.enforcement_state
            .set_next_holder_commit_num(commitment_number + 1, info2)?;

        // These calls are guaranteed to pass the commitment_number
        // check because we just advanced it to the right spot above.
        let next_holder_commitment_point = self
            .get_per_commitment_point(commitment_number + 1)
            .unwrap();
        let maybe_old_secret = if commitment_number >= 1 {
            Some(
                self.get_per_commitment_secret(commitment_number - 1)
                    .unwrap(),
            )
        } else {
            None
        };

        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;

        Ok((next_holder_commitment_point, maybe_old_secret))
    }

    pub fn validate_counterparty_revocation(
        &mut self,
        revoke_num: u64,
        old_secret: &SecretKey,
    ) -> Result<(), Status> {
        let validator = self
            .node
            .upgrade()
            .unwrap()
            .validator_factory
            .make_validator(self.network());

        // TODO - need to store the revealed secret.

        let estate = &mut self.enforcement_state;
        validator.validate_counterparty_revocation(&estate, revoke_num, old_secret)?;
        estate.set_next_counterparty_revoke_num(revoke_num + 1)?;

        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;
        Ok(())
    }

    pub fn sign_holder_commitment_tx(
        &self,
        tx: &bitcoin::Transaction,
        output_witscripts: &Vec<Vec<u8>>,
        commitment_number: u64,
        feerate_per_kw: u32,
        offered_htlcs: Vec<HTLCInfo2>,
        received_htlcs: Vec<HTLCInfo2>,
    ) -> Result<Signature, Status> {
        let validator = self
            .node
            .upgrade()
            .unwrap()
            .validator_factory
            .make_validator(self.network());

        validator.validate_sign_holder_commitment_tx(&self.enforcement_state, commitment_number)?;

        let (recomposed_tx, _info2) = self.make_recomposed_holder_commitment_tx(
            tx,
            output_witscripts,
            commitment_number,
            feerate_per_kw,
            offered_htlcs,
            received_htlcs,
        )?;
        let htlcs = recomposed_tx.htlcs();

        // We provide a dummy signature for the remote, since we don't require that sig
        // to be passed in to this call.  It would have been better if HolderCommitmentTransaction
        // didn't require the remote sig.
        // TODO consider if we actually want the sig for policy checks
        let dummy_sig = Secp256k1::new().sign(
            &secp256k1::Message::from_slice(&[42; 32]).unwrap(),
            &SecretKey::from_slice(&[42; 32]).unwrap(),
        );
        let mut htlc_dummy_sigs = Vec::with_capacity(htlcs.len());
        htlc_dummy_sigs.resize(htlcs.len(), dummy_sig);

        // Holder commitments need an extra wrapper for the LDK signature routine.
        let recomposed_holder_tx = HolderCommitmentTransaction::new(
            recomposed_tx,
            dummy_sig,
            htlc_dummy_sigs,
            &self.keys.pubkeys().funding_pubkey,
            &self.keys.counterparty_pubkeys().funding_pubkey,
        );

        // Sign the recomposed commitment.
        let sigs = self
            .keys
            .sign_holder_commitment_and_htlcs(&recomposed_holder_tx, &self.secp_ctx)
            .map_err(|_| internal_error("failed to sign"))?;

        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;

        // Discard the htlc signatures for now.
        Ok(sigs.0)
    }

    /// Phase 1
    pub fn sign_mutual_close_tx(
        &mut self,
        tx: &bitcoin::Transaction,
        funding_amount_sat: u64,
    ) -> Result<Signature, Status> {
        let sig = sign_commitment(
            &self.secp_ctx,
            &self.keys,
            &self.setup.counterparty_points.funding_pubkey,
            &tx,
            funding_amount_sat,
        )
        .map_err(|_| Status::internal("failed to sign"))?;
        self.enforcement_state.mutual_close_signed = true;
        trace_enforcement_state!(&self.enforcement_state);
        self.persist()?;
        Ok(sig)
    }

    /// Phase 1
    pub fn sign_holder_htlc_tx(
        &self,
        tx: &bitcoin::Transaction,
        commitment_number: u64,
        opt_per_commitment_point: Option<PublicKey>,
        redeemscript: &Script,
        htlc_amount_sat: u64,
        output_witscript: &Script,
    ) -> Result<Signature, Status> {
        let per_commitment_point = if opt_per_commitment_point.is_some() {
            opt_per_commitment_point.unwrap()
        } else {
            self.get_per_commitment_point(commitment_number)?
        };

        let txkeys = self
            .make_holder_tx_keys(&per_commitment_point)
            .expect("failed to make txkeys");

        self.sign_htlc_tx(
            tx,
            &per_commitment_point,
            redeemscript,
            htlc_amount_sat,
            output_witscript,
            false, // is_counterparty
            txkeys,
        )
    }

    /// Phase 1
    pub fn sign_counterparty_htlc_tx(
        &self,
        tx: &bitcoin::Transaction,
        remote_per_commitment_point: &PublicKey,
        redeemscript: &Script,
        htlc_amount_sat: u64,
        output_witscript: &Script,
    ) -> Result<Signature, Status> {
        let txkeys = self
            .make_counterparty_tx_keys(&remote_per_commitment_point)
            .expect("failed to make txkeys");

        self.sign_htlc_tx(
            tx,
            remote_per_commitment_point,
            redeemscript,
            htlc_amount_sat,
            output_witscript,
            true, // is_counterparty
            txkeys,
        )
    }

    pub fn sign_htlc_tx(
        &self,
        tx: &bitcoin::Transaction,
        per_commitment_point: &PublicKey,
        redeemscript: &Script,
        htlc_amount_sat: u64,
        output_witscript: &Script,
        is_counterparty: bool,
        txkeys: TxCreationKeys,
    ) -> Result<Signature, Status> {
        let validator = self
            .node
            .upgrade()
            .unwrap()
            .validator_factory
            .make_validator(self.network());

        let (feerate_per_kw, htlc, recomposed_tx_sighash) = validator.decode_and_validate_htlc_tx(
            is_counterparty,
            &self.setup,
            &txkeys,
            tx,
            &redeemscript,
            htlc_amount_sat,
            output_witscript,
        )?;

        // TODO(devrandom) - obtain current_height so that we can validate the HTLC CLTV
        let state = ValidatorState { current_height: 0 };
        validator
            .validate_htlc_tx(&self.setup, &state, is_counterparty, &htlc, feerate_per_kw)
            .map_err(|ve| {
                debug!(
                    "VALIDATION FAILED: {}\n\
                     setup={:#?}\n\
                     state={:#?}\n\
                     is_counterparty={}\n\
                     tx={:#?}\n\
                     htlc={:#?}\n\
                     feerate_per_kw={}",
                    ve,
                    &self.setup,
                    &state,
                    is_counterparty,
                    &tx,
                    DebugHTLCOutputInCommitment(&htlc),
                    feerate_per_kw,
                );
                ve
            })?;

        let htlc_privkey = derive_private_key(
            &self.secp_ctx,
            &per_commitment_point,
            &self.keys.htlc_base_key,
        )
        .map_err(|_| Status::internal("failed to derive key"))?;

        let htlc_sighash = Message::from_slice(&recomposed_tx_sighash[..])
            .map_err(|_| Status::internal("failed to sighash recomposed"))?;

        Ok(self.secp_ctx.sign(&htlc_sighash, &htlc_privkey))
    }

    // TODO(devrandom) key leaking from this layer
    pub fn get_unilateral_close_key(
        &self,
        commitment_point: &Option<PublicKey>,
    ) -> Result<SecretKey, Status> {
        Ok(match commitment_point {
            Some(commitment_point) => {
                derive_private_key(&self.secp_ctx, &commitment_point, &self.keys.payment_key)
                    .map_err(|err| {
                        Status::internal(format!("derive_private_key failed: {}", err))
                    })?
            }
            None => {
                // option_static_remotekey in effect
                self.keys.payment_key.clone()
            }
        })
    }
}

pub fn channel_nonce_to_id(nonce: &Vec<u8>) -> ChannelId {
    // Impedance mismatch - we want a 32 byte channel ID for internal use
    // Hash the client supplied channel nonce
    let hash = Sha256Hash::hash(nonce);
    ChannelId(hash.into_inner())
}