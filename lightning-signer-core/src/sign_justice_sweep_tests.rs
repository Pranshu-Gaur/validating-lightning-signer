#[cfg(test)]
mod tests {
    use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
    use bitcoin::{self, OutPoint, Script, Transaction, TxIn, TxOut, Txid};
    use lightning::ln::chan_utils::get_revokeable_redeemscript;
    use test_env_log::test;

    use crate::channel::{Channel, ChannelBase};
    use crate::node::SpendType::{P2shP2wpkh, P2wpkh};
    use crate::util::crypto_utils::{
        derive_private_revocation_key, derive_public_key, derive_revocation_pubkey,
        signature_to_bitcoin_vec,
    };
    use crate::util::key_utils::{make_test_key, make_test_pubkey};
    use crate::util::status::{Code, Status};
    use crate::util::test_utils::*;

    fn make_test_justice_sweep_tx(
        txid: Txid,
        vout: u32,
        script_pubkey: Script,
        amount_sat: u64,
    ) -> Transaction {
        Transaction {
            version: 2,
            lock_time: 0,
            input: vec![TxIn {
                previous_output: OutPoint { txid, vout },
                script_sig: Script::new(),
                sequence: 0x_ffff_ffff_u32,
                witness: vec![],
            }],
            output: vec![TxOut {
                script_pubkey: script_pubkey,
                value: amount_sat,
            }],
        }
    }

    fn sign_justice_sweep_with_mutators<MakeDestination, InputMutator>(
        make_dest: MakeDestination,
        mutate_signing_input: InputMutator,
    ) -> Result<(), Status>
    where
        MakeDestination: Fn(&TestNodeContext) -> (Script, Vec<u32>),
        InputMutator:
            Fn(&mut Channel, &mut Transaction, &mut usize, &mut SecretKey, &mut Script, &mut u64),
    {
        let (node, setup, channel_id, offered_htlcs, received_htlcs) =
            sign_commitment_tx_with_mutators_setup();

        let node_ctx = TestNodeContext {
            node,
            secp_ctx: Secp256k1::signing_only(),
        };
        let chan_ctx = TestChannelContext {
            channel_id,
            setup: setup.clone(),
            counterparty_keys: make_test_counterparty_keys(
                &node_ctx,
                &channel_id,
                setup.channel_value_sat,
            ),
        };

        let (sig, tx, revocation_secret, input, redeemscript, amount_sat) = node_ctx
            .node
            .with_ready_channel(&chan_ctx.channel_id, |chan| {
                let secp_ctx = Secp256k1::new();

                // These need to match sign_commitment_tx_with_mutators_setup() ...
                let commit_num = 23;
                let feerate_per_kw = 5_000;
                let to_broadcaster = 1_979_997;
                let to_countersignatory = 1_000_000;

                chan.set_next_holder_commit_num_for_testing(commit_num + 2);

                let remote_per_commitment_point = make_test_pubkey(10);
                let keys = chan.make_counterparty_tx_keys(&remote_per_commitment_point)?;
                let htlcs =
                    Channel::htlcs_info2_to_oic(offered_htlcs.clone(), received_htlcs.clone());
                let commitment_tx = chan.make_counterparty_commitment_tx_with_keys(
                    keys.clone(),
                    commit_num,
                    feerate_per_kw,
                    to_countersignatory,
                    to_broadcaster,
                    htlcs.clone(),
                );
                let built_commit = commitment_tx.trust().built_transaction().clone();
                let built_commit_txid = &built_commit.txid;
                let built_commit_tx = &built_commit.transaction;

                let to_local_outndx = 4;
                let mut amount_sat = built_commit_tx.output[to_local_outndx].value;
                assert_eq!(amount_sat, 1_979_997);

                let per_commitment_point = chan.get_per_commitment_point(commit_num)?;
                let per_commitment_secret = chan.get_per_commitment_secret(commit_num)?;

                let (revocation_base_point, revocation_base_secret) = make_test_key(42);

                let revocation_pubkey = derive_revocation_pubkey(
                    &secp_ctx,
                    &per_commitment_point,
                    &revocation_base_point,
                )
                .expect("revocation_pubkey");

                let mut revocation_secret = derive_private_revocation_key(
                    &secp_ctx,
                    &per_commitment_secret,
                    &revocation_base_secret,
                )
                .expect("revocation_secret");

                let (script_pubkey, wallet_path) = make_dest(&node_ctx);

                let mut input = 0;
                let fee = 1_000;
                let mut tx = make_test_justice_sweep_tx(
                    built_commit_txid.clone(),
                    to_local_outndx as u32,
                    script_pubkey,
                    amount_sat - fee,
                );

                let delayed_payment_base = make_test_pubkey(2);
                let delayed_payment_pubkey =
                    derive_public_key(&secp_ctx, &per_commitment_point, &delayed_payment_base)
                        .expect("delayed_payment_pubkey");
                let mut redeemscript = get_revokeable_redeemscript(
                    &revocation_pubkey,
                    setup.holder_selected_contest_delay,
                    &delayed_payment_pubkey,
                );

                mutate_signing_input(
                    chan,
                    &mut tx,
                    &mut input,
                    &mut revocation_secret,
                    &mut redeemscript,
                    &mut amount_sat,
                );

                let sig = chan.sign_justice_sweep(
                    &tx,
                    input,
                    &revocation_secret,
                    &redeemscript,
                    amount_sat,
                    &wallet_path,
                )?;

                Ok((sig, tx, revocation_secret, input, redeemscript, amount_sat))
            })?;

        let revocation_point = PublicKey::from_secret_key(&node_ctx.secp_ctx, &revocation_secret);
        let pubkey =
            get_channel_revocation_pubkey(&node_ctx.node, &chan_ctx.channel_id, &revocation_point);

        check_signature(
            &tx,
            input,
            signature_to_bitcoin_vec(sig),
            &pubkey,
            amount_sat,
            &redeemscript,
        );

        Ok(())
    }

    // policy-justice-sweep-destination-allowlisted
    #[test]
    fn sign_justice_to_local_wallet_p2wpkh_success() {
        assert_status_ok!(sign_justice_sweep_with_mutators(
            |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
            |_chan, _tx, _input, _commit_num, _redeemscript, _amount_sat| {},
        ));
    }

    // policy-justice-sweep-destination-allowlisted
    #[test]
    fn sign_justice_to_local_wallet_p2shwpkh_success() {
        assert_status_ok!(sign_justice_sweep_with_mutators(
            |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2shP2wpkh) },
            |_chan, _tx, _input, _commit_num, _redeemscript, _amount_sat| {},
        ));
    }

    // policy-justice-sweep-destination-allowlisted
    #[test]
    fn sign_justice_to_local_allowlist_p2wpkh_success() {
        assert_status_ok!(sign_justice_sweep_with_mutators(
            |node_ctx| { make_test_nonwallet_dest(node_ctx, 3, P2wpkh) },
            |chan, _tx, _input, _commit_num, _redeemscript, _amount_sat| {
                chan.node
                    .upgrade()
                    .unwrap()
                    .add_allowlist(&vec![
                        "tb1qg975h6gdx5mryeac72h6lj2nzygugxhyk6dnhr".to_string()
                    ])
                    .expect("add_allowlist");
            },
        ));
    }

    // policy-justice-sweep-destination-allowlisted
    #[test]
    fn sign_justice_to_local_allowlist_p2shwpkh_success() {
        assert_status_ok!(sign_justice_sweep_with_mutators(
            |node_ctx| { make_test_nonwallet_dest(node_ctx, 3, P2shP2wpkh) },
            |chan, _tx, _input, _commit_num, _redeemscript, _amount_sat| {
                chan.node
                    .upgrade()
                    .unwrap()
                    .add_allowlist(&vec!["2MspRgcQvaVN2RkpumN1X8GkzsE7BVTTb6y".to_string()])
                    .expect("add_allowlist");
            },
        ));
    }

    // policy-justice-sweep-destination-allowlisted
    #[test]
    fn sign_justice_to_local_with_unknown_dest() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_nonwallet_dest(node_ctx, 3, P2shP2wpkh) },
                |_chan, _tx, _input, _commit_num, _redeemscript, _amount_sat| {},
            ),
            "policy failure: validate_justice_sweep: destination is not in wallet or allowlist"
        );
    }

    // policy-justice-sweep-destination-allowlisted
    #[test]
    fn sign_justice_sweep_with_wrong_wallet_path() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| {
                    // Build the dest from index 19, but report index 21.
                    (make_test_wallet_dest(node_ctx, 19, P2wpkh).0, vec![21])
                },
                |_chan, _tx, _input, _commit_num, _redeemscript, _amount_sat| {},
            ),
            "policy failure: validate_justice_sweep: destination is not in wallet or allowlist"
        );
    }

    #[test]
    fn sign_justice_sweep_with_bad_num_inputs() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                    tx.input.push(tx.input[0].clone());
                },
            ),
            "transaction format: validate_justice_sweep: \
             bad number of justice sweep inputs: 2 != 1"
        );
    }

    #[test]
    fn sign_justice_sweep_with_bad_num_outputs() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                    tx.output.push(tx.output[0].clone());
                },
            ),
            "transaction format: validate_justice_sweep: \
             bad number of justice sweep outputs: 2 != 1"
        );
    }

    #[test]
    fn sign_justice_sweep_with_bad_input_index() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, _tx, input, _commit_num, _redeemscript, _amount_sat| {
                    *input = 1;
                },
            ),
            "transaction format: validate_justice_sweep: bad input index: 1 != 0"
        );
    }

    // policy-justice-sweep-version
    #[test]
    fn sign_justice_sweep_with_bad_version() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                    tx.version = 3;
                },
            ),
            "transaction format: validate_justice_sweep: bad justice sweep version: 3"
        );
    }

    // policy-justice-sweep-locktime
    #[test]
    fn sign_justice_sweep_with_bad_locktime() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                    tx.lock_time = 1_000_000;
                },
            ),
            "transaction format: validate_justice_sweep: bad justice sweep locktime: 1000000 > 0"
        );
    }

    // policy-justice-sweep-sequence
    #[test]
    fn sign_justice_sweep_with_rbf_sequence_success() {
        assert_status_ok!(sign_justice_sweep_with_mutators(
            |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
            |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                tx.input[0].sequence = 0x_ffff_fffd_u32;
            },
        ));
    }

    // policy-justice-sweep-sequence
    #[test]
    fn sign_justice_sweep_with_bad_sequence() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                    tx.input[0].sequence = 42;
                },
            ),
            "transaction format: validate_justice_sweep: \
             bad justice sweep sequence: 42 not in [0, 4294967293, 4294967295]"
        );
    }

    #[test]
    fn sign_justice_sweep_with_fee_underflow() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, _tx, _input, _commit_num, _redeemscript, amount_sat| {
                    *amount_sat -= 100_000;
                },
            ),
            "policy failure: justice sweep fee underflow: 1879997 - 1978997"
        );
    }

    // policy-justice-sweep-fee-range
    #[test]
    fn sign_justice_sweep_with_fee_too_small() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, amount_sat| {
                    *amount_sat = tx.output[0].value; // fee = 0
                },
            ),
            "policy failure: validate_fee: validate_justice_sweep: fee below minimum: 0 < 100"
        );
    }

    // policy-justice-sweep-fee-range
    #[test]
    fn sign_justice_sweep_with_fee_too_large() {
        assert_failed_precondition_err!(
            sign_justice_sweep_with_mutators(
                |node_ctx| { make_test_wallet_dest(node_ctx, 19, P2wpkh) },
                |_chan, tx, _input, _commit_num, _redeemscript, _amount_sat| {
                    tx.output[0].value = 1_000;
                },
            ),
            "policy failure: validate_fee: validate_justice_sweep: \
             fee above maximum: 1978997 > 46000"
        );
    }
}
