use kv::{Bucket, Config, Json, Store, TransactionError};

use bitcoin::secp256k1::PublicKey;
use lightning_signer::chain::tracker::ChainTracker;

use lightning_signer::channel::{Channel, ChannelId, ChannelStub};
use lightning_signer::monitor::ChainMonitor;
use lightning_signer::node::NodeConfig;
use lightning_signer::persist::model::{
    ChannelEntry as CoreChannelEntry, NodeEntry as CoreNodeEntry,
};
use lightning_signer::persist::Persist;
use lightning_signer::policy::validator::EnforcementState;
use log::error;

use crate::persist::model::ChainTrackerEntry;
use crate::persist::model::NodeChannelId;
use crate::persist::model::{AllowlistItemEntry, ChannelEntry, NodeEntry};

/// A persister that uses the kv crate and JSON serialization for values.
pub struct KVJsonPersister<'a> {
    pub node_bucket: Bucket<'a, Vec<u8>, Json<NodeEntry>>,
    pub channel_bucket: Bucket<'a, NodeChannelId, Json<ChannelEntry>>,
    pub allowlist_bucket: Bucket<'a, Vec<u8>, Json<AllowlistItemEntry>>,
    pub chain_tracker_bucket: Bucket<'a, Vec<u8>, Json<ChainTrackerEntry>>,
}

impl KVJsonPersister<'_> {
    pub fn new(path: &str) -> Self {
        let cfg = Config::new(path);
        let store = Store::new(cfg).expect("create store");
        let node_bucket = store.bucket(Some("nodes")).expect("create node bucket");
        let channel_bucket = store.bucket(Some("channels")).expect("create channel bucket");
        let allowlist_bucket = store.bucket(Some("allowlists")).expect("create allowlist bucket");
        let chain_tracker_bucket =
            store.bucket(Some("chain_tracker")).expect("create chain tracker bucket");
        Self { node_bucket, channel_bucket, allowlist_bucket, chain_tracker_bucket }
    }
}

impl<'a> Persist for KVJsonPersister<'a> {
    fn new_node(&self, node_id: &PublicKey, config: &NodeConfig, seed: &[u8]) {
        let key = node_id.serialize().to_vec();
        assert!(!self.node_bucket.contains(key.clone()).unwrap());
        let entry = NodeEntry {
            seed: seed.to_vec(),
            key_derivation_style: config.key_derivation_style as u8,
            network: config.network.to_string(),
        };
        self.node_bucket.set(key, Json(entry)).expect("insert node");
        self.node_bucket.flush().expect("flush");
    }

    fn delete_node(&self, node_id: &PublicKey) {
        for item_res in self.channel_bucket.iter_prefix(NodeChannelId::new_prefix(node_id)) {
            let id: NodeChannelId = item_res.unwrap().key().unwrap();
            self.channel_bucket.remove(id).unwrap();
        }
        let key = node_id.serialize().to_vec();
        self.node_bucket.remove(key.clone()).unwrap();
        self.chain_tracker_bucket.remove(key).unwrap();
    }

    fn new_channel(&self, node_id: &PublicKey, stub: &ChannelStub) -> Result<(), ()> {
        let channel_value_satoshis = 0; // TODO not known yet

        self.channel_bucket
            .transaction(|txn| {
                let id = NodeChannelId::new(node_id, &stub.id0);
                let entry = ChannelEntry {
                    nonce: stub.nonce.clone(),
                    channel_value_satoshis,
                    channel_setup: None,
                    id: None,
                    enforcement_state: EnforcementState::new(0),
                };
                if txn.get(id.clone()).unwrap().is_some() {
                    return Err(TransactionError::Abort(kv::Error::Message(
                        "already exists".to_string(),
                    )));
                }
                txn.set(id, Json(entry)).expect("insert channel");
                Ok(())
            })
            .expect("new transaction");
        self.channel_bucket.flush().expect("flush");
        Ok(())
    }

    fn new_chain_tracker(&self, node_id: &PublicKey, tracker: &ChainTracker<ChainMonitor>) {
        let key = node_id.serialize().to_vec();
        assert!(!self.chain_tracker_bucket.contains(key.clone()).unwrap());
        self.chain_tracker_bucket.set(key, Json(tracker.into())).expect("insert chain tracker");
        self.chain_tracker_bucket.flush().expect("flush");
    }

    fn update_tracker(
        &self,
        node_id: &PublicKey,
        tracker: &ChainTracker<ChainMonitor>,
    ) -> Result<(), ()> {
        let key = node_id.serialize().to_vec();
        self.chain_tracker_bucket.set(key, Json(tracker.into())).expect("update chain tracker");
        self.chain_tracker_bucket.flush().expect("flush");
        Ok(())
    }

    fn get_tracker(&self, node_id: &PublicKey) -> Result<ChainTracker<ChainMonitor>, ()> {
        let key = node_id.serialize().to_vec();
        let value = self.chain_tracker_bucket.get(key).unwrap().ok_or_else(|| ())?;
        Ok(value.0.into())
    }

    fn update_channel(&self, node_id: &PublicKey, channel: &Channel) -> Result<(), ()> {
        let channel_value_satoshis = channel.setup.channel_value_sat;

        self.channel_bucket
            .transaction(|txn| {
                let node_channel_id = NodeChannelId::new(node_id, &channel.id0);
                let entry = ChannelEntry {
                    nonce: channel.nonce.clone(),
                    channel_value_satoshis,
                    channel_setup: Some(channel.setup.clone()),
                    id: channel.id,
                    enforcement_state: channel.enforcement_state.clone(),
                };
                if txn.get(node_channel_id.clone()).unwrap().is_none() {
                    return Err(TransactionError::Abort(kv::Error::Message(
                        "not found".to_string(),
                    )));
                }
                let json = Json(entry);
                txn.set(node_channel_id, json).expect("update channel");
                Ok(())
            })
            .expect("update transaction");
        self.channel_bucket.flush().expect("flush");
        Ok(())
    }

    fn get_channel(
        &self,
        node_id: &PublicKey,
        channel_id: &ChannelId,
    ) -> Result<CoreChannelEntry, ()> {
        let id = NodeChannelId::new(node_id, channel_id);
        let value = self.channel_bucket.get(id).unwrap().ok_or_else(|| ())?;
        let entry = CoreChannelEntry::from(value.0);
        Ok(entry)
    }

    fn get_node_channels(&self, node_id: &PublicKey) -> Vec<(ChannelId, CoreChannelEntry)> {
        let mut res = Vec::new();
        for item_res in self.channel_bucket.iter_prefix(NodeChannelId::new_prefix(node_id)) {
            let item = item_res.unwrap();
            let value: Json<ChannelEntry> = item.value().unwrap();
            let entry = CoreChannelEntry::from(value.0);
            let key: NodeChannelId = item.key().unwrap();
            res.push((key.channel_id(), entry));
        }
        res
    }

    fn update_node_allowlist(&self, node_id: &PublicKey, allowlist: Vec<String>) -> Result<(), ()> {
        let key = node_id.serialize().to_vec();
        let entry = AllowlistItemEntry { allowlist };
        self.allowlist_bucket.set(key, Json(entry)).expect("update transaction");
        self.allowlist_bucket.flush().expect("flush");

        Ok(())
    }

    fn get_node_allowlist(&self, node_id: &PublicKey) -> Vec<String> {
        let key = node_id.serialize().to_vec();
        let entry = self.allowlist_bucket.get(key);
        if entry.is_err() {
            // TODO make this fatal
            error!("allowlist entry error {:?}", entry.err());
            return vec![];
        }
        let entry2 = entry.unwrap();
        if entry2.is_none() {
            return vec![];
        }
        entry2.unwrap().0.allowlist
    }

    fn get_nodes(&self) -> Vec<(PublicKey, CoreNodeEntry)> {
        let mut res = Vec::new();
        for item_res in self.node_bucket.iter() {
            let item = item_res.unwrap();
            let value: Json<NodeEntry> = item.value().unwrap();
            let entry = CoreNodeEntry::from(value.0);
            let key: Vec<u8> = item.key().unwrap();
            res.push((PublicKey::from_slice(key.as_slice()).unwrap(), entry));
        }
        res
    }

    fn clear_database(&self) {
        self.channel_bucket.clear().unwrap();
        self.node_bucket.clear().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::lightning;
    use lightning::chain::keysinterface::InMemorySigner;
    use lightning::util::ser::Writeable;
    use tempfile::TempDir;
    use test_log::test;

    use lightning_signer::channel::{channel_nonce_to_id, ChannelSlot};
    use lightning_signer::node::Node;
    use lightning_signer::policy::simple_validator::SimpleValidatorFactory;
    use lightning_signer::util::test_utils::*;

    use crate::persist::ser_util::VecWriter;

    use super::*;

    fn make_temp_persister<'a>() -> (KVJsonPersister<'a>, TempDir, String) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_owned();
        let path_str = path.to_str().unwrap();

        let persister = KVJsonPersister::new(path_str);
        persister.clear_database();
        (persister, dir, path_str.to_string())
    }

    #[test]
    fn round_trip_signer_test() {
        let channel_nonce = "nonce0".as_bytes().to_vec();
        let channel_id0 = channel_nonce_to_id(&channel_nonce);
        let validator_factory = Arc::new(SimpleValidatorFactory::new());

        let (node_id, node_arc, stub, seed) = make_node_and_channel(&channel_nonce, channel_id0);

        let node = &*node_arc;

        let (temp_dir, path) = {
            let (persister, temp_dir, path) = make_temp_persister();
            let persister: Arc<dyn Persist> = Arc::new(persister);
            persister.new_node(&node_id, &TEST_NODE_CONFIG, &seed);
            persister.new_chain_tracker(&node_id, &node.get_tracker());
            persister.new_channel(&node_id, &stub).unwrap();

            let nodes = Node::restore_nodes(Arc::clone(&persister), validator_factory.clone());
            let restored_node = nodes.get(&node_id).unwrap();

            {
                let slot = restored_node.get_channel(&stub.id0).unwrap();

                let guard = slot.lock().unwrap();
                if let ChannelSlot::Stub(s) = &*guard {
                    check_signer_roundtrip(&stub.keys, &s.keys);
                } else {
                    panic!()
                }
            }

            // Ready the channel
            {
                let dummy_pubkey = make_dummy_pubkey(0x12);
                let setup = create_test_channel_setup(dummy_pubkey);

                let channel_nonce1 = "nonce1".as_bytes().to_vec();
                let channel_id1 = channel_nonce_to_id(&channel_nonce1);

                let channel =
                    node.ready_channel(channel_id0, Some(channel_id1), setup, &vec![]).unwrap();
                persister.update_channel(&node_id, &channel).unwrap();

                let nodes = Node::restore_nodes(Arc::clone(&persister), validator_factory.clone());
                let restored_node_arc = nodes.get(&node_id).unwrap();
                let slot = restored_node_arc.get_channel(&stub.id0).unwrap();
                assert!(node.channels().contains_key(&channel_id0));
                assert!(node.channels().contains_key(&channel_id1));
                let guard = slot.lock().unwrap();
                if let ChannelSlot::Ready(s) = &*guard {
                    check_signer_roundtrip(&channel.keys, &s.keys);
                } else {
                    panic!()
                }
            }
            (temp_dir, path)
        };

        {
            let persister1 = KVJsonPersister::new(path.as_str());
            let nodes = persister1.get_nodes();
            assert_eq!(nodes.len(), 1);
        }

        drop(temp_dir);

        {
            let persister1 = KVJsonPersister::new(path.as_str());
            let nodes = persister1.get_nodes();
            assert_eq!(nodes.len(), 0);
        }
    }

    fn check_signer_roundtrip(existing_signer: &InMemorySigner, signer: &InMemorySigner) {
        let mut existing_w = VecWriter(Vec::new());
        existing_signer.write(&mut existing_w).unwrap();
        let mut w = VecWriter(Vec::new());
        signer.write(&mut w).unwrap();
        assert_eq!(existing_w.0, w.0);
    }
}
