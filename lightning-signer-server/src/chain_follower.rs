use std::collections::BTreeSet as OrderedSet;
use std::fmt::{self, Display, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::{task, time};
use url::Url;

use bitcoin::util::merkleblock::PartialMerkleTree;
use bitcoin::{Block, BlockHash, OutPoint, Transaction, TxOut, Txid};

use bitcoind_client::{BitcoindClient, BlockSource, Error};
use lightning_signer::node::Node;
use lightning_signer::wallet::Wallet;

#[allow(unused_imports)]
use lightning_signer::{debug_vals, short_function, vals_str};
#[allow(unused_imports)]
use log::{debug, error, info, trace};

const UPDATE_INTERVAL_MSEC: u64 = 100;

/// Follows the longest chain and feeds proofs of watched changes to ChainTracker.
#[allow(dead_code)]
pub struct ChainFollower {
    node: Arc<Node>,
    rpc_url: Url,
    client: Option<BitcoindClient>,
    state: State,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
enum State {
    Disconnected,
    Scanning,
    Synced,
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            State::Disconnected => write!(f, "disconnected"),
            State::Scanning => write!(f, "scanning"),
            State::Synced => write!(f, "synced"),
        }
    }
}

#[derive(PartialEq)]
enum ScheduleNext {
    Pause,
    Immediate,
    // Terminate,
}

macro_rules! abbrev {
    ($str: expr, $sz: expr) => {{
        format!("{}..", $str.to_string()[0..$sz].to_string())
    }};
}

impl ChainFollower {
    pub async fn new(node: Arc<Node>, rpc_url: &Url) -> Arc<Mutex<ChainFollower>> {
        info!(
            "node {} creating {} follower using {}",
            abbrev!(node.get_id(), 12),
            node.network(),
            rpc_url
        );
        let cf_arc = Arc::new(Mutex::new(ChainFollower {
            node,
            rpc_url: rpc_url.clone(),
            client: None,
            state: State::Disconnected,
        }));
        let cf_arc_clone = Arc::clone(&cf_arc);
        task::spawn(async move {
            ChainFollower::run(&cf_arc_clone).await;
        });
        cf_arc
    }

    async fn run(cf_arc: &Arc<Mutex<ChainFollower>>) {
        let mut interval = time::interval(Duration::from_millis(UPDATE_INTERVAL_MSEC));
        loop {
            interval.tick().await;
            loop {
                let mut cf = cf_arc.lock().await;
                match cf.update().await {
                    Ok(next) => {
                        if next == ScheduleNext::Pause {
                            break;
                        }
                        // otherwise loop immediately
                    }
                    Err(err) => {
                        error!("node {}: {}", abbrev!(cf.node.get_id(), 12), err);
                        cf.client = None;
                        cf.state = State::Disconnected;
                        break; // Would a shorter pause be better here?
                    }
                }
            }
        }
    }

    // IMPORTANT - the caller must not hold the Node Arc Mutex because deadlock!
    async fn update(&mut self) -> Result<ScheduleNext, Error> {
        match self.state {
            State::Disconnected => {
                let client = BitcoindClient::new(
                    self.rpc_url.host_str().expect("rpc host").to_owned(),
                    self.rpc_url.port().expect("rpc port"),
                    self.rpc_url.username().to_owned(),
                    self.rpc_url.password().to_owned().expect("rpc password").to_owned(),
                )
                .await?;
                debug!("node {} connected", abbrev!(self.node.get_id(), 12));
                self.client = Some(client);
                self.state = State::Scanning;
                Ok(ScheduleNext::Immediate)
            }
            State::Scanning | State::Synced => self.advance().await,
        }
    }

    async fn advance(&mut self) -> Result<ScheduleNext, Error> {
        let client = self.client.as_ref().unwrap();

        // Fetch the current tip from the tracker
        let (height0, hash0) = {
            // Confine the scope of the tracker, can't span awaits ...
            let tracker = self.node.get_tracker();
            let height = tracker.height();
            let header = tracker.tip();
            let hash = header.block_hash();
            (height, hash)
        };

        // Fetch the next block hash from bitcoind
        let hash = match client.get_block_hash(height0 + 1).await? {
            None => {
                // No new block, confirm that the current block still matches
                match client.get_block_hash(height0).await? {
                    None => {
                        // Our current block is gone, must be reorg in progress
                        return self.remove_block(height0, hash0).await;
                    }
                    Some(check_hash0) =>
                        if check_hash0 != hash0 {
                            return self.remove_block(height0, hash0).await;
                        },
                }
                // Current top block matches
                if self.state != State::Synced {
                    info!("node {} synced at height {}", abbrev!(self.node.get_id(), 12), height0);
                    self.state = State::Synced;
                }
                return Ok(ScheduleNext::Pause);
            }
            Some(hash) => {
                // There is a new block
                hash
            }
        };

        self.state = State::Scanning;

        // Fetch the next block from bitcoind
        let block = client.get_block(&hash).await?;

        // Is the new block on top of our current tip?
        if block.header.prev_blockhash != hash0 {
            // Reorg, remove the last block
            return self.remove_block(height0, hash0).await;
        }

        // Add this block
        let height = height0 + 1;
        if height % 2016 == 0 {
            info!("node {} at height {}", abbrev!(self.node.get_id(), 12), height);
        }

        let mut tracker = self.node.get_tracker();
        let (txid_watches, outpoint_watches) = tracker.get_all_forward_watches();
        let (txs, proof) = build_proof(&block, &txid_watches, &outpoint_watches);
        // debug!("node {} at height {} adding {}", abbrev!(self.node.get_id(), 12), height, hash);
        let rv = tracker.add_block(block.header, txs, proof);
        if rv.is_err() {
            panic!(
                "tracker.add_block failed at height {}: {:?}: {}",
                height,
                rv.unwrap_err(),
                vals_str!(block.header)
            );
        }
        Ok(ScheduleNext::Immediate)
    }

    async fn remove_block(
        &mut self,
        height0: u32,
        hash0: BlockHash,
    ) -> Result<ScheduleNext, Error> {
        debug!(
            "node {} reorg at height {}, removing hash {}",
            abbrev!(self.node.get_id(), 12),
            height0,
            abbrev!(hash0, 12),
        );
        let client = self.client.as_ref().unwrap();
        let block = client.get_block(&hash0).await?;
        let mut tracker = self.node.get_tracker();
        let (txid_watches, outpoint_watches) = tracker.get_all_reverse_watches();
        let (txs, proof) = build_proof(&block, &txid_watches, &outpoint_watches);
        // The tracker will reverse the txs in remove_block, so leave normal order here.
        let rv = tracker.remove_block(txs, proof);
        if rv.is_err() {
            panic!(
                "tracker.remove_block failed at height {}: {:?}: {}",
                height0,
                rv.unwrap_err(),
                vals_str!(block.header)
            );
        }
        Ok(ScheduleNext::Immediate)
    }
}

fn build_proof(
    block: &Block,
    txid_watches: &Vec<Txid>,
    outpoint_watches: &Vec<OutPoint>,
) -> (Vec<Transaction>, Option<PartialMerkleTree>) {
    let watched_txids = OrderedSet::from_iter(txid_watches.iter());
    let mut watched_outpoints = OrderedSet::from_iter(outpoint_watches.iter().cloned());
    let mut txids = vec![];
    let mut matches = vec![];
    let mut matched_txs = vec![];
    for tx in block.txdata.iter() {
        let txid = tx.txid();
        txids.push(txid);
        if watched_txids.contains(&txid)
            || tx.input.iter().any(|inp| watched_outpoints.contains(&inp.previous_output))
        {
            matches.push(true);
            matched_txs.push(tx.clone());
            // We need to watch the outputs of this transaction in case a subsequent
            // transaction in the block spends them.
            let additional_watches: Vec<OutPoint> = (0..tx.output.len() as u32)
                .into_iter()
                .map(|vout| OutPoint { txid, vout })
                .collect();
            watched_outpoints.extend(additional_watches.into_iter());
        } else {
            matches.push(false);
        }
    }

    if matched_txs.is_empty() {
        (vec![], None)
    } else {
        let proof = PartialMerkleTree::from_txids(&txids, &matches);
        (matched_txs, Some(proof))
    }
}

// A transaction summary suitable for debugging follower activities
#[derive(Debug)]
#[allow(dead_code)]
struct DebugTransactionSummary {
    txid: Txid,
    inputs: Vec<OutPoint>,
    outputs: Vec<TxOut>,
}

#[allow(dead_code)]
fn debug_block_txs(block: &Block) -> Vec<DebugTransactionSummary> {
    block.txdata.iter().map(|tx| debug_transaction_summary(&tx)).collect::<Vec<_>>()
}

#[allow(dead_code)]
fn debug_transaction_summary(tx: &Transaction) -> DebugTransactionSummary {
    DebugTransactionSummary {
        txid: tx.txid(),
        inputs: tx.input.iter().map(|inp| inp.previous_output).collect(),
        outputs: tx.output.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use bitcoin::{Block, BlockHeader, OutPoint, TxIn, TxMerkleNode, TxOut};

    use test_env_log::test;

    fn make_tx(previous_outputs: Vec<OutPoint>, outputs: Vec<TxOut>) -> Transaction {
        Transaction {
            version: 2,
            lock_time: 0,
            input: previous_outputs
                .iter()
                .map(|previous_output| TxIn {
                    previous_output: *previous_output,
                    script_sig: Default::default(),
                    sequence: 0,
                    witness: vec![],
                })
                .collect(),
            output: outputs,
        }
    }

    fn make_blockheader() -> BlockHeader {
        BlockHeader {
            version: 0,
            prev_blockhash: Default::default(),
            merkle_root: TxMerkleNode::from_str(
                "0377d5ba2c6e0f7aeaebb3caa6cd05b8b9b8ba60d0554e53b7a327ffdaa7433a",
            )
            .unwrap(),
            time: 0,
            bits: 0,
            nonce: 0,
        }
    }

    fn make_block() -> Block {
        Block {
            header: make_blockheader(),
            txdata: vec![
                make_tx( // coinbase txid: 9310175d644aab7b9337ce806a4c7e27cdd815611085eab1a20c746a11742114
                    vec![OutPoint::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000000:4294967295").unwrap()],
                    vec![
                        TxOut { value: 5000002055, script_pubkey: Default::default() }
                    ],
                ),
                make_tx( // watched by txid_watch txid: 71ce38d3be3f07ac707d1c348bfa976f6d8060c28dce533914bcdc6b7e38d091
                    vec![OutPoint::from_str(
                        "7b2c3d17a43ac15757cc2b768d602d1a9333269802b8ee9fab375ea25a0509c8:0").unwrap()],
                    vec![
                        TxOut { value: 9993618, script_pubkey: Default::default() },
                    ],
                ),
                make_tx( // watched by watch txid: ea2e897dbe842b0a3645d7297070579f42ce234cfb1da25f9195f4259496a1a6
                    vec![OutPoint::from_str(
                        "5df6e0e2761359d30a8275058e299fcc0381534545f55cf43e41983f5d4c9456:1").unwrap()],
                    vec![
                        TxOut { value: 123456789, script_pubkey: Default::default() },
                    ],
                ),
                make_tx( // ignored txid: c8f94365a721c1637cacf44e734358d595dca8372a5d90e098482ed8f8d52cac
                    vec![OutPoint::from_str(
                        "f5864806e3565c34d1b41e716f72609d00b55ea5eac5b924c9719a842ef42206:1").unwrap()],
                    vec![
                        TxOut { value: 1000000000, script_pubkey: Default::default() },
                    ],
                ),
                make_tx( // ignored txid: 96b6df8e0cbb9919414c96a56a9b52e7299c9e394b46533c7dea2d14843a457b
                    vec![OutPoint::from_str(
                        "80b7d8a82d5d5bf92905b06f2014dd699e03837ca172e3a59d51426ebbe3e7f5:0").unwrap()],
                    vec![
                        TxOut { value: 2000000000, script_pubkey: Default::default() },
                    ],
                ),
                make_tx( // additional from watch txid: bfb031d4bb062a26561b828d686d0a30b2083d26463924060b638985348870cf
                    vec![OutPoint::from_str(
                        "ea2e897dbe842b0a3645d7297070579f42ce234cfb1da25f9195f4259496a1a6:0").unwrap()],
                    vec![
                        TxOut { value: 3000000000, script_pubkey: Default::default() },
                    ],
                ),
                make_tx( // additional from txid_watch txid: 17299ca25bd0dd92ef3e75f0aac5b04ad7477565e09e3bf4cc4fa2816f5d00d4
                    vec![OutPoint::from_str(
                        "71ce38d3be3f07ac707d1c348bfa976f6d8060c28dce533914bcdc6b7e38d091:0").unwrap()],
                    vec![
                        TxOut { value: 4000000000, script_pubkey: Default::default() },
                    ],
                ),
            ],
        }
    }

    #[test]
    fn build_proof_with_empty_block() {
        let block = Block { header: make_blockheader(), txdata: vec![] };
        assert_eq!(build_proof(&block, &vec![], &vec![]), (vec![], None));
    }

    #[test]
    fn build_proof_with_empty_watches() {
        assert_eq!(build_proof(&make_block(), &vec![], &vec![]), (vec![], None));
    }

    #[test]
    fn build_proof_with_txid_watch() {
        let block = make_block();
        let txid_watches = vec![block.txdata[1].txid()];
        let (txs, proof) = build_proof(&block, &txid_watches, &vec![]);
        assert_eq!(txs, vec![block.txdata[1].clone(), block.txdata[6].clone()]);
        assert!(proof.is_some());
        let mut matches = Vec::new();
        let mut indexes = Vec::new();
        let root = proof.unwrap().extract_matches(&mut matches, &mut indexes).unwrap();
        assert_eq!(root, block.header.merkle_root);
        assert_eq!(matches, vec![block.txdata[1].txid(), block.txdata[6].txid()]);
        assert_eq!(indexes, vec![1, 6]);
    }

    #[test]
    fn build_proof_with_outpoint_watch() {
        let block = make_block();
        let outpoint_watches = vec![block.txdata[2].input[0].previous_output];
        let (txs, proof) = build_proof(&block, &vec![], &outpoint_watches);
        assert_eq!(txs, vec![block.txdata[2].clone(), block.txdata[5].clone()]);
        assert!(proof.is_some());
        let mut matches = Vec::new();
        let mut indexes = Vec::new();
        let root = proof.unwrap().extract_matches(&mut matches, &mut indexes).unwrap();
        assert_eq!(root, block.header.merkle_root);
        assert_eq!(matches, vec![block.txdata[2].txid(), block.txdata[5].txid()]);
        assert_eq!(indexes, vec![2, 5]);
    }
}
