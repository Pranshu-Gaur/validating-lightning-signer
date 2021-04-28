//! A bunch of useful utilities for building networks of nodes and exchanging messages between
//! nodes for functional tests.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;

use bitcoin;
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;
use bitcoin::secp256k1::key::PublicKey;
use bitcoin::{Block, Network, Transaction, TxOut};
use bitcoin::hashes::sha256::Hash as Sha256;
use bitcoin::hashes::Hash;
use chain::transaction::OutPoint;
use lightning::chain;
use lightning::ln;
use lightning::ln::channelmanager::{PaymentSecret, ChainParameters};
use lightning::routing::network_graph::NetGraphMsgHandler;
use lightning::routing::router::{get_route, Route};
use lightning::util;
use lightning::util::config::UserConfig;
use lightning::util::test_utils;
use ln::channelmanager::{ChannelManager, PaymentHash, PaymentPreimage};
use ln::features::InitFeatures;
use ln::msgs;
use ln::msgs::{ChannelMessageHandler, RoutingMessageHandler};
use util::events::{Event, EventsProvider, MessageSendEvent, MessageSendEventsProvider};

use crate::util::loopback::{LoopbackChannelSigner, LoopbackSignerKeysInterface};
use crate::util::test_utils::{TestChainMonitor, TestPersister};
use lightning::chain::Listen;
use bitcoin::blockdata::constants::genesis_block;
use lightning::ln::functional_test_utils::ConnectStyle;

pub const CHAN_CONFIRM_DEPTH: u32 = 10;

/// Mine the given transaction in the next block and then mine CHAN_CONFIRM_DEPTH - 1 blocks on
/// top, giving the given transaction CHAN_CONFIRM_DEPTH confirmations.
pub fn confirm_transaction<'a, 'b, 'c, 'd>(node: &'a Node<'b, 'c, 'd>, tx: &Transaction) {
    confirm_transaction_at(node, tx, node.best_block_info().1 + 1);
    connect_blocks(node, CHAN_CONFIRM_DEPTH - 1);
}
/// Mine a signle block containing the given transaction
pub fn mine_transaction<'a, 'b, 'c, 'd>(node: &'a Node<'b, 'c, 'd>, tx: &Transaction) {
    let height = node.best_block_info().1 + 1;
    confirm_transaction_at(node, tx, height);
}
/// Mine the given transaction at the given height, mining blocks as required to build to that
/// height
pub fn confirm_transaction_at<'a, 'b, 'c, 'd>(node: &'a Node<'b, 'c, 'd>, tx: &Transaction, conf_height: u32) {
    let first_connect_height = node.best_block_info().1 + 1;
    assert!(first_connect_height <= conf_height);
    if conf_height - first_connect_height >= 1 {
        connect_blocks(node, conf_height - first_connect_height);
    }
    let mut block = Block {
        header: BlockHeader { version: 0x20000000, prev_blockhash: node.best_block_hash(), merkle_root: Default::default(), time: 42, bits: 42, nonce: 42 },
        txdata: Vec::new(),
    };
    for _ in 0..*node.network_chan_count.borrow() { // Make sure we don't end up with channels at the same short id by offsetting by chan_count
        block.txdata.push(Transaction { version: 0, lock_time: 0, input: Vec::new(), output: Vec::new() });
    }
    block.txdata.push(tx.clone());
    connect_block(node, &block);
}

pub fn connect_blocks<'a, 'b, 'c, 'd>(node: &'a Node<'b, 'c, 'd>, depth: u32) -> BlockHash {
    let skip_intermediaries = match *node.connect_style.borrow() {
        ConnectStyle::BestBlockFirstSkippingBlocks|ConnectStyle::TransactionsFirstSkippingBlocks => true,
        _ => false,
    };

    let mut block = Block {
        header: BlockHeader { version: 0x2000000, prev_blockhash: node.best_block_hash(), merkle_root: Default::default(), time: 42, bits: 42, nonce: 42 },
        txdata: vec![],
    };
    assert!(depth >= 1);
    for _ in 0..depth - 1 {
        do_connect_block(node, &block, skip_intermediaries);
        block = Block {
            header: BlockHeader { version: 0x20000000, prev_blockhash: block.header.block_hash(), merkle_root: Default::default(), time: 42, bits: 42, nonce: 42 },
            txdata: vec![],
        };
    }
    connect_block(node, &block);
    block.header.block_hash()
}

pub fn connect_block<'a, 'b, 'c, 'd>(node: &'a Node<'b, 'c, 'd>, block: &Block) {
    do_connect_block(node, block, false);
}

fn do_connect_block<'a, 'b, 'c, 'd>(node: &'a Node<'b, 'c, 'd>, block: &Block, skip_manager: bool) {
    let txdata: Vec<_> = block.txdata.iter().enumerate().collect();
    let height = node.best_block_info().1 + 1;
    node.chain_monitor.chain_monitor.block_connected(&block.header, &txdata, height);
    if !skip_manager {
        match *node.connect_style.borrow() {
            ConnectStyle::BestBlockFirst|ConnectStyle::BestBlockFirstSkippingBlocks => {
                node.node.update_best_block(&block.header, height);
                node.node.transactions_confirmed(&block.header, height, &block.txdata.iter().enumerate().collect::<Vec<_>>());
            },
            ConnectStyle::TransactionsFirst|ConnectStyle::TransactionsFirstSkippingBlocks => {
                node.node.transactions_confirmed(&block.header, height, &block.txdata.iter().enumerate().collect::<Vec<_>>());
                node.node.update_best_block(&block.header, height);
            },
            ConnectStyle::FullBlockViaListen => {
                Listen::block_connected(node.node, &block, height);
            }
        }
    }

    // We really want here test_process_background_events, but it's crate-private
    node.node.timer_tick_occurred();
    node.blocks.borrow_mut().push((block.header, height));
}


// BEGIN NOT TESTED
pub fn disconnect_block<'a, 'b, 'c, 'd>(
    node: &'a Node<'b, 'c, 'd>,
    header: &BlockHeader,
    height: u32,
) {
    node.chain_monitor
        .chain_monitor
        .block_disconnected(header, height);
    node.node.block_disconnected(header, height);
}
// END NOT TESTED

pub struct TestChanMonCfg {
    pub tx_broadcaster: test_utils::TestBroadcaster,
    pub fee_estimator: test_utils::TestFeeEstimator,
    pub chain_source: test_utils::TestChainSource,
    pub persister: TestPersister,
    pub logger: test_utils::TestLogger,
}

pub struct NodeCfg<'a> {
    pub chain_source: &'a test_utils::TestChainSource,
    pub tx_broadcaster: &'a test_utils::TestBroadcaster,
    pub fee_estimator: &'a test_utils::TestFeeEstimator,
    pub chain_monitor: TestChainMonitor<'a>,
    pub keys_manager: LoopbackSignerKeysInterface,
    pub logger: &'a test_utils::TestLogger,
    pub node_seed: [u8; 32],
}

pub struct Node<'a, 'b: 'a, 'c: 'b> {
    pub chain_source: &'c test_utils::TestChainSource,
    pub tx_broadcaster: &'c test_utils::TestBroadcaster,
    pub chain_monitor: &'b TestChainMonitor<'c>,
    pub keys_manager: &'b LoopbackSignerKeysInterface,
    pub node: &'a ChannelManager<
        LoopbackChannelSigner,
        &'b TestChainMonitor<'c>,
        &'c test_utils::TestBroadcaster,
        &'b LoopbackSignerKeysInterface,
        &'c test_utils::TestFeeEstimator,
        &'c test_utils::TestLogger,
    >,
    pub net_graph_msg_handler:
        NetGraphMsgHandler<&'c test_utils::TestChainSource, &'c test_utils::TestLogger>,
    pub node_seed: [u8; 32],
    pub network_payment_count: Rc<RefCell<u8>>,
    pub network_chan_count: Rc<RefCell<u32>>,
    pub logger: &'c test_utils::TestLogger,
    pub blocks: RefCell<Vec<(BlockHeader, u32)>>,
    pub connect_style: Rc<RefCell<ConnectStyle>>,
}
impl<'a, 'b, 'c> Node<'a, 'b, 'c> {
    pub fn best_block_hash(&self) -> BlockHash {
        self.blocks.borrow_mut().last().unwrap().0.block_hash()
    }
    pub fn best_block_info(&self) -> (BlockHash, u32) {
        self.blocks.borrow_mut().last().map(|(a, b)| (a.block_hash(), *b)).unwrap()
    }
}

impl<'a, 'b, 'c> Drop for Node<'a, 'b, 'c> {
    fn drop(&mut self) {
        if !::std::thread::panicking() {
            // Check that we processed all pending events
            assert!(self.node.get_and_clear_pending_msg_events().is_empty());
            assert!(self.node.get_and_clear_pending_events().is_empty());
            assert!(self.chain_monitor.added_monitors.lock().unwrap().is_empty());
        }
    }
}

// BEGIN NOT TESTED
pub fn create_chan_between_nodes<'a, 'b, 'c, 'd>(
    node_a: &'a Node<'b, 'c, 'd>,
    node_b: &'a Node<'b, 'c, 'd>,
    a_flags: InitFeatures,
    b_flags: InitFeatures,
) -> (
    msgs::ChannelAnnouncement,
    msgs::ChannelUpdate,
    msgs::ChannelUpdate,
    [u8; 32],
    Transaction,
) {
    create_chan_between_nodes_with_value(node_a, node_b, 100000, 10001, a_flags, b_flags)
}
// END NOT TESTED

pub fn create_chan_between_nodes_with_value<'a, 'b, 'c, 'd>(
    node_a: &'a Node<'b, 'c, 'd>,
    node_b: &'a Node<'b, 'c, 'd>,
    channel_value: u64,
    push_msat: u64,
    a_flags: InitFeatures,
    b_flags: InitFeatures,
) -> (
    msgs::ChannelAnnouncement,
    msgs::ChannelUpdate,
    msgs::ChannelUpdate,
    [u8; 32],
    Transaction,
) {
    let (funding_locked, channel_id, tx) = create_chan_between_nodes_with_value_a(
        node_a,
        node_b,
        channel_value,
        push_msat,
        a_flags,
        b_flags,
    );
    let (announcement, as_update, bs_update) =
        create_chan_between_nodes_with_value_b(node_a, node_b, &funding_locked);
    (announcement, as_update, bs_update, channel_id, tx)
}

macro_rules! get_revoke_commit_msgs {
    ($node: expr, $node_id: expr) => {{
        let events = $node.node.get_and_clear_pending_msg_events();
        assert_eq!(events.len(), 2);
        (
            match events[0] {
                MessageSendEvent::SendRevokeAndACK {
                    ref node_id,
                    ref msg,
                } => {
                    assert_eq!(*node_id, $node_id);
                    (*msg).clone()
                }
                _ => panic!("Unexpected event"),
            },
            match events[1] {
                MessageSendEvent::UpdateHTLCs {
                    ref node_id,
                    ref updates,
                } => {
                    assert_eq!(*node_id, $node_id);
                    assert!(updates.update_add_htlcs.is_empty());
                    assert!(updates.update_fulfill_htlcs.is_empty());
                    assert!(updates.update_fail_htlcs.is_empty());
                    assert!(updates.update_fail_malformed_htlcs.is_empty());
                    assert!(updates.update_fee.is_none());
                    updates.commitment_signed.clone()
                }
                _ => panic!("Unexpected event"),
            },
        )
    }};
}

macro_rules! get_event_msg {
    ($node: expr, $event_type: path, $node_id: expr) => {{
        let events = $node.node.get_and_clear_pending_msg_events();
        assert_eq!(events.len(), 1);
        match events[0] {
            $event_type {
                ref node_id,
                ref msg,
            } => {
                assert_eq!(*node_id, $node_id);
                (*msg).clone()
            }
            _ => panic!("Unexpected event"),
        }
    }};
}

#[macro_export]
macro_rules! get_local_commitment_txn {
    ($node: expr, $channel_id: expr) => {{
        let mut monitors = $node.chain_monitor.chain_monitor.monitors.write().unwrap();
        let mut commitment_txn = None;
        for (funding_txo, monitor) in monitors.iter_mut() {
            if funding_txo.to_channel_id() == $channel_id {
                commitment_txn =
                    Some(monitor.unsafe_get_latest_holder_commitment_txn(&$node.logger));
                break;
            }
        }
        commitment_txn.unwrap()
    }};
}

/// Check that a channel's closing channel update has been broadcasted, and optionally
/// check whether an error message event has occurred.
#[macro_export]
macro_rules! check_closed_broadcast {
	($node: expr, $with_error_msg: expr) => {{
        use lightning::util::events::MessageSendEvent;
        use lightning::ln::msgs::ErrorAction;

		let events = $node.node.get_and_clear_pending_msg_events();
		assert_eq!(events.len(), if $with_error_msg { 2 } else { 1 });
		match events[0] {
			MessageSendEvent::BroadcastChannelUpdate { ref msg } => {
				assert_eq!(msg.contents.flags & 2, 2);
			},
			_ => panic!("Unexpected event"),
		}
		if $with_error_msg {
			match events[1] {
				MessageSendEvent::HandleError { action: ErrorAction::SendErrorMessage { ref msg }, node_id: _ } => {
					// TODO: Check node_id
					Some(msg.clone())
				},
				_ => panic!("Unexpected event"),
			}
		} else { None }
	}}
}

#[macro_export]
macro_rules! check_added_monitors {
    ($node: expr, $count: expr) => {{
        let mut added_monitors = $node.chain_monitor.added_monitors.lock().unwrap();
        assert_eq!(added_monitors.len(), $count);
        added_monitors.clear();
    }};
}

pub fn create_funding_transaction<'a, 'b, 'c>(node: &Node<'a, 'b, 'c>, expected_chan_value: u64, expected_user_chan_id: u64) -> ([u8; 32], Transaction, OutPoint) {
    let chan_id = *node.network_chan_count.borrow();

    let events = node.node.get_and_clear_pending_events();
    assert_eq!(events.len(), 1);
    match events[0] {
        Event::FundingGenerationReady { ref temporary_channel_id, ref channel_value_satoshis, ref output_script, user_channel_id } => {
            assert_eq!(*channel_value_satoshis, expected_chan_value);
            assert_eq!(user_channel_id, expected_user_chan_id);

            let tx = Transaction { version: chan_id as i32, lock_time: 0, input: Vec::new(), output: vec![TxOut {
                value: *channel_value_satoshis, script_pubkey: output_script.clone(),
            }]};
            let funding_outpoint = OutPoint { txid: tx.txid(), index: 0 };
            (*temporary_channel_id, tx, funding_outpoint)
        },
        _ => panic!("Unexpected event"),
    }
}

pub fn create_chan_between_nodes_with_value_init<'a, 'b, 'c>(node_a: &Node<'a, 'b, 'c>, node_b: &Node<'a, 'b, 'c>, channel_value: u64, push_msat: u64, a_flags: InitFeatures, b_flags: InitFeatures) -> Transaction {
    node_a.node.create_channel(node_b.node.get_our_node_id(), channel_value, push_msat, 42, None).unwrap();
    node_b.node.handle_open_channel(&node_a.node.get_our_node_id(), a_flags, &get_event_msg!(node_a, MessageSendEvent::SendOpenChannel, node_b.node.get_our_node_id()));
    node_a.node.handle_accept_channel(&node_b.node.get_our_node_id(), b_flags, &get_event_msg!(node_b, MessageSendEvent::SendAcceptChannel, node_a.node.get_our_node_id()));

    let (temporary_channel_id, tx, funding_output) = create_funding_transaction(node_a, channel_value, 42);

    node_a.node.funding_transaction_generated(&temporary_channel_id, tx.clone()).unwrap();
    check_added_monitors!(node_a, 0);

    node_b.node.handle_funding_created(&node_a.node.get_our_node_id(), &get_event_msg!(node_a, MessageSendEvent::SendFundingCreated, node_b.node.get_our_node_id()));
    {
        let mut added_monitors = node_b.chain_monitor.added_monitors.lock().unwrap();
        assert_eq!(added_monitors.len(), 1);
        assert_eq!(added_monitors[0].0, funding_output);
        added_monitors.clear();
    }

    node_a.node.handle_funding_signed(&node_b.node.get_our_node_id(), &get_event_msg!(node_b, MessageSendEvent::SendFundingSigned, node_a.node.get_our_node_id()));
    {
        let mut added_monitors = node_a.chain_monitor.added_monitors.lock().unwrap();
        assert_eq!(added_monitors.len(), 1);
        assert_eq!(added_monitors[0].0, funding_output);
        added_monitors.clear();
    }

    let events_4 = node_a.node.get_and_clear_pending_events();
    assert_eq!(events_4.len(), 0);

    assert_eq!(node_a.tx_broadcaster.txn_broadcasted.lock().unwrap().len(), 1);
    assert_eq!(node_a.tx_broadcaster.txn_broadcasted.lock().unwrap()[0], tx);
    node_a.tx_broadcaster.txn_broadcasted.lock().unwrap().clear();

    tx
}

pub fn create_chan_between_nodes_with_value_confirm_first<'a, 'b, 'c, 'd>(node_recv: &'a Node<'b, 'c, 'c>, node_conf: &'a Node<'b, 'c, 'd>, tx: &Transaction, conf_height: u32) {
    confirm_transaction_at(node_conf, tx, conf_height);
    connect_blocks(node_conf, CHAN_CONFIRM_DEPTH - 1);
    node_recv.node.handle_funding_locked(&node_conf.node.get_our_node_id(), &get_event_msg!(node_conf, MessageSendEvent::SendFundingLocked, node_recv.node.get_our_node_id()));
}

pub fn create_chan_between_nodes_with_value_confirm_second<'a, 'b, 'c>(node_recv: &Node<'a, 'b, 'c>, node_conf: &Node<'a, 'b, 'c>) -> ((msgs::FundingLocked, msgs::AnnouncementSignatures), [u8; 32]) {
    let channel_id;
    let events_6 = node_conf.node.get_and_clear_pending_msg_events();
    assert_eq!(events_6.len(), 2);
    ((match events_6[0] {
        MessageSendEvent::SendFundingLocked { ref node_id, ref msg } => {
            channel_id = msg.channel_id.clone();
            assert_eq!(*node_id, node_recv.node.get_our_node_id());
            msg.clone()
        },
        _ => panic!("Unexpected event"),
    }, match events_6[1] {
        MessageSendEvent::SendAnnouncementSignatures { ref node_id, ref msg } => {
            assert_eq!(*node_id, node_recv.node.get_our_node_id());
            msg.clone()
        },
        _ => panic!("Unexpected event"),
    }), channel_id)
}

pub fn create_chan_between_nodes_with_value_confirm<'a, 'b, 'c, 'd>(node_a: &'a Node<'b, 'c, 'd>, node_b: &'a Node<'b, 'c, 'd>, tx: &Transaction) -> ((msgs::FundingLocked, msgs::AnnouncementSignatures), [u8; 32]) {
    let conf_height = std::cmp::max(node_a.best_block_info().1 + 1, node_b.best_block_info().1 + 1);
    create_chan_between_nodes_with_value_confirm_first(node_a, node_b, tx, conf_height);
    confirm_transaction_at(node_a, tx, conf_height);
    connect_blocks(node_a, CHAN_CONFIRM_DEPTH - 1);
    create_chan_between_nodes_with_value_confirm_second(node_b, node_a)
}

pub fn create_chan_between_nodes_with_value_a<'a, 'b, 'c, 'd>(node_a: &'a Node<'b, 'c, 'd>, node_b: &'a Node<'b, 'c, 'd>, channel_value: u64, push_msat: u64, a_flags: InitFeatures, b_flags: InitFeatures) -> ((msgs::FundingLocked, msgs::AnnouncementSignatures), [u8; 32], Transaction) {
    let tx = create_chan_between_nodes_with_value_init(node_a, node_b, channel_value, push_msat, a_flags, b_flags);
    let (msgs, chan_id) = create_chan_between_nodes_with_value_confirm(node_a, node_b, &tx);
    (msgs, chan_id, tx)
}

pub fn create_chan_between_nodes_with_value_b<'a, 'b, 'c>(node_a: &Node<'a, 'b, 'c>, node_b: &Node<'a, 'b, 'c>, as_funding_msgs: &(msgs::FundingLocked, msgs::AnnouncementSignatures)) -> (msgs::ChannelAnnouncement, msgs::ChannelUpdate, msgs::ChannelUpdate) {
    node_b.node.handle_funding_locked(&node_a.node.get_our_node_id(), &as_funding_msgs.0);
    let bs_announcement_sigs = get_event_msg!(node_b, MessageSendEvent::SendAnnouncementSignatures, node_a.node.get_our_node_id());
    node_b.node.handle_announcement_signatures(&node_a.node.get_our_node_id(), &as_funding_msgs.1);

    let events_7 = node_b.node.get_and_clear_pending_msg_events();
    assert_eq!(events_7.len(), 1);
    let (announcement, bs_update) = match events_7[0] {
        MessageSendEvent::BroadcastChannelAnnouncement { ref msg, ref update_msg } => {
            (msg, update_msg)
        },
        _ => panic!("Unexpected event"),
    };

    node_a.node.handle_announcement_signatures(&node_b.node.get_our_node_id(), &bs_announcement_sigs);
    let events_8 = node_a.node.get_and_clear_pending_msg_events();
    assert_eq!(events_8.len(), 1);
    let as_update = match events_8[0] {
        MessageSendEvent::BroadcastChannelAnnouncement { ref msg, ref update_msg } => {
            assert!(*announcement == *msg);
            assert_eq!(update_msg.contents.short_channel_id, announcement.contents.short_channel_id);
            assert_eq!(update_msg.contents.short_channel_id, bs_update.contents.short_channel_id);
            update_msg
        },
        _ => panic!("Unexpected event"),
    };

    *node_a.network_chan_count.borrow_mut() += 1;

    ((*announcement).clone(), (*as_update).clone(), (*bs_update).clone())
}

pub fn create_announced_chan_between_nodes<'a, 'b, 'c, 'd>(nodes: &'a Vec<Node<'b, 'c, 'd>>, a: usize, b: usize, a_flags: InitFeatures, b_flags: InitFeatures) -> (msgs::ChannelUpdate, msgs::ChannelUpdate, [u8; 32], Transaction) {
    create_announced_chan_between_nodes_with_value(nodes, a, b, 100000, 10001, a_flags, b_flags)
}

pub fn create_announced_chan_between_nodes_with_value<'a, 'b, 'c, 'd>(nodes: &'a Vec<Node<'b, 'c, 'd>>, a: usize, b: usize, channel_value: u64, push_msat: u64, a_flags: InitFeatures, b_flags: InitFeatures) -> (msgs::ChannelUpdate, msgs::ChannelUpdate, [u8; 32], Transaction) {
    let chan_announcement = create_chan_between_nodes_with_value(&nodes[a], &nodes[b], channel_value, push_msat, a_flags, b_flags);
    update_nodes_with_chan_announce(nodes, a, b, &chan_announcement.0, &chan_announcement.1, &chan_announcement.2);
    (chan_announcement.1, chan_announcement.2, chan_announcement.3, chan_announcement.4)
}

pub fn update_nodes_with_chan_announce<'a, 'b, 'c, 'd>(nodes: &'a Vec<Node<'b, 'c, 'd>>, a: usize, b: usize, ann: &msgs::ChannelAnnouncement, upd_1: &msgs::ChannelUpdate, upd_2: &msgs::ChannelUpdate) {
    nodes[a].node.broadcast_node_announcement([0, 0, 0], [0; 32], Vec::new());
    let a_events = nodes[a].node.get_and_clear_pending_msg_events();
    assert_eq!(a_events.len(), 1);
    let a_node_announcement = match a_events[0] {
        MessageSendEvent::BroadcastNodeAnnouncement { ref msg } => {
            (*msg).clone()
        },
        _ => panic!("Unexpected event"),
    };

    nodes[b].node.broadcast_node_announcement([1, 1, 1], [1; 32], Vec::new());
    let b_events = nodes[b].node.get_and_clear_pending_msg_events();
    assert_eq!(b_events.len(), 1);
    let b_node_announcement = match b_events[0] {
        MessageSendEvent::BroadcastNodeAnnouncement { ref msg } => {
            (*msg).clone()
        },
        _ => panic!("Unexpected event"),
    };

    for node in nodes {
        assert!(node.net_graph_msg_handler.handle_channel_announcement(ann).unwrap());
        node.net_graph_msg_handler.handle_channel_update(upd_1).unwrap();
        node.net_graph_msg_handler.handle_channel_update(upd_2).unwrap();
        node.net_graph_msg_handler.handle_node_announcement(&a_node_announcement).unwrap();
        node.net_graph_msg_handler.handle_node_announcement(&b_node_announcement).unwrap();
    }
}

#[macro_export]
macro_rules! check_spends {
    ($tx: expr, $spends_tx: expr) => {{
        $tx.verify(|out_point| {
            if out_point.txid == $spends_tx.txid() {
                $spends_tx.output.get(out_point.vout as usize).cloned()
            } else {
                None // NOT TESTED
            }
        })
        .unwrap();
    }};
}

macro_rules! get_closing_signed_broadcast {
    ($node: expr, $dest_pubkey: expr) => {{
        let events = $node.get_and_clear_pending_msg_events();
        assert!(events.len() == 1 || events.len() == 2);
        (
            match events[events.len() - 1] {
                MessageSendEvent::BroadcastChannelUpdate { ref msg } => msg.clone(),
                _ => panic!("Unexpected event"),
            },
            if events.len() == 2 {
                match events[0] {
                    MessageSendEvent::SendClosingSigned {
                        ref node_id,
                        ref msg,
                    } => {
                        assert_eq!(*node_id, $dest_pubkey);
                        Some(msg.clone())
                    }
                    _ => panic!("Unexpected event"),
                }
            } else {
                None
            },
        )
    }};
}

pub fn close_channel<'a, 'b, 'c>(
    outbound_node: &Node<'a, 'b, 'c>,
    inbound_node: &Node<'a, 'b, 'c>,
    channel_id: &[u8; 32],
    funding_tx: Transaction,
    close_inbound_first: bool,
) -> (msgs::ChannelUpdate, msgs::ChannelUpdate, Transaction) {
    let (node_a, broadcaster_a, struct_a) = if close_inbound_first {
        (
            &inbound_node.node,
            &inbound_node.tx_broadcaster,
            inbound_node,
        )
    } else {
        // BEGIN NOT TESTED
        (
            &outbound_node.node,
            &outbound_node.tx_broadcaster,
            outbound_node,
        )
        // END NOT TESTED
    };
    let (node_b, broadcaster_b) = if close_inbound_first {
        (&outbound_node.node, &outbound_node.tx_broadcaster)
    } else {
        (&inbound_node.node, &inbound_node.tx_broadcaster) // NOT TESTED
    };
    let (tx_a, tx_b);

    node_a.close_channel(channel_id).unwrap();
    node_b.handle_shutdown(
        &node_a.get_our_node_id(),
        &InitFeatures::known(),
        &get_event_msg!(
            struct_a,
            MessageSendEvent::SendShutdown,
            node_b.get_our_node_id()
        ),
    );

    let events_1 = node_b.get_and_clear_pending_msg_events();
    assert!(events_1.len() >= 1);
    let shutdown_b = match events_1[0] {
        MessageSendEvent::SendShutdown {
            ref node_id,
            ref msg,
        } => {
            assert_eq!(node_id, &node_a.get_our_node_id());
            msg.clone()
        }
        _ => panic!("Unexpected event"), // NOT TESTED
    };

    let closing_signed_b = if !close_inbound_first {
        assert_eq!(events_1.len(), 1); // NOT TESTED
        None // NOT TESTED
    } else {
        Some(match events_1[1] {
            MessageSendEvent::SendClosingSigned {
                ref node_id,
                ref msg,
            } => {
                assert_eq!(node_id, &node_a.get_our_node_id());
                msg.clone()
            }
            _ => panic!("Unexpected event"), // NOT TESTED
        })
    };

    node_a.handle_shutdown(&node_b.get_our_node_id(), &InitFeatures::known(), &shutdown_b);
    let (as_update, bs_update) = if close_inbound_first {
        assert!(node_a.get_and_clear_pending_msg_events().is_empty());
        node_a.handle_closing_signed(&node_b.get_our_node_id(), &closing_signed_b.unwrap());
        assert_eq!(broadcaster_a.txn_broadcasted.lock().unwrap().len(), 1);
        tx_a = broadcaster_a.txn_broadcasted.lock().unwrap().remove(0);
        let (as_update, closing_signed_a) =
            get_closing_signed_broadcast!(node_a, node_b.get_our_node_id());

        node_b.handle_closing_signed(&node_a.get_our_node_id(), &closing_signed_a.unwrap());
        let (bs_update, none_b) = get_closing_signed_broadcast!(node_b, node_a.get_our_node_id());
        assert!(none_b.is_none());
        assert_eq!(broadcaster_b.txn_broadcasted.lock().unwrap().len(), 1);
        tx_b = broadcaster_b.txn_broadcasted.lock().unwrap().remove(0);
        (as_update, bs_update)
    } else {
        // BEGIN NOT TESTED
        let closing_signed_a = get_event_msg!(
            struct_a,
            MessageSendEvent::SendClosingSigned,
            node_b.get_our_node_id()
        );

        node_b.handle_closing_signed(&node_a.get_our_node_id(), &closing_signed_a);
        assert_eq!(broadcaster_b.txn_broadcasted.lock().unwrap().len(), 1);
        tx_b = broadcaster_b.txn_broadcasted.lock().unwrap().remove(0);
        let (bs_update, closing_signed_b) =
            get_closing_signed_broadcast!(node_b, node_a.get_our_node_id());

        node_a.handle_closing_signed(&node_b.get_our_node_id(), &closing_signed_b.unwrap());
        let (as_update, none_a) = get_closing_signed_broadcast!(node_a, node_b.get_our_node_id());
        assert!(none_a.is_none());
        assert_eq!(broadcaster_a.txn_broadcasted.lock().unwrap().len(), 1);
        tx_a = broadcaster_a.txn_broadcasted.lock().unwrap().remove(0);
        (as_update, bs_update)
        // END NOT TESTED
    };
    assert_eq!(tx_a, tx_b);
    check_spends!(tx_a, funding_tx);

    (as_update, bs_update, tx_a)
}

pub struct SendEvent {
    pub node_id: PublicKey,
    pub msgs: Vec<msgs::UpdateAddHTLC>,
    pub commitment_msg: msgs::CommitmentSigned,
}

impl SendEvent {
    pub fn from_commitment_update(
        node_id: PublicKey,
        updates: msgs::CommitmentUpdate,
    ) -> SendEvent {
        assert!(updates.update_fulfill_htlcs.is_empty());
        assert!(updates.update_fail_htlcs.is_empty());
        assert!(updates.update_fail_malformed_htlcs.is_empty());
        assert!(updates.update_fee.is_none());
        SendEvent {
            node_id: node_id,
            msgs: updates.update_add_htlcs,
            commitment_msg: updates.commitment_signed,
        }
    }

    pub fn from_event(event: MessageSendEvent) -> SendEvent {
        match event {
            MessageSendEvent::UpdateHTLCs { node_id, updates } => {
                SendEvent::from_commitment_update(node_id, updates)
            }
            _ => panic!("Unexpected event type!"), // NOT TESTED
        }
    }

    // BEGIN NOT TESTED
    pub fn from_node<'a, 'b, 'c>(node: &Node<'a, 'b, 'c>) -> SendEvent {
        let mut events = node.node.get_and_clear_pending_msg_events();
        assert_eq!(events.len(), 1);
        SendEvent::from_event(events.pop().unwrap())
    }
    // END NOT TESTED
}

macro_rules! commitment_signed_dance {
    ($node_a: expr, $node_b: expr, $commitment_signed: expr, $fail_backwards: expr, true /* skip last step */) => {{
        check_added_monitors!($node_a, 0);
        assert!($node_a.node.get_and_clear_pending_msg_events().is_empty());
        $node_a
            .node
            .handle_commitment_signed(&$node_b.node.get_our_node_id(), &$commitment_signed);
        check_added_monitors!($node_a, 1);
        commitment_signed_dance!($node_a, $node_b, (), $fail_backwards, true, false);
    }};
    ($node_a: expr, $node_b: expr, (), $fail_backwards: expr, true /* skip last step */, true /* return extra message */, true /* return last RAA */) => {{
        let (as_revoke_and_ack, as_commitment_signed) =
            get_revoke_commit_msgs!($node_a, $node_b.node.get_our_node_id());
        check_added_monitors!($node_b, 0);
        assert!($node_b.node.get_and_clear_pending_msg_events().is_empty());
        $node_b
            .node
            .handle_revoke_and_ack(&$node_a.node.get_our_node_id(), &as_revoke_and_ack);
        assert!($node_b.node.get_and_clear_pending_msg_events().is_empty());
        check_added_monitors!($node_b, 1);
        $node_b
            .node
            .handle_commitment_signed(&$node_a.node.get_our_node_id(), &as_commitment_signed);
        let (bs_revoke_and_ack, extra_msg_option) = {
            let events = $node_b.node.get_and_clear_pending_msg_events();
            assert!(events.len() <= 2);
            (
                match events[0] {
                    MessageSendEvent::SendRevokeAndACK {
                        ref node_id,
                        ref msg,
                    } => {
                        assert_eq!(*node_id, $node_a.node.get_our_node_id());
                        (*msg).clone()
                    }
                    _ => panic!("Unexpected event"),
                },
                events.get(1).map(|e| e.clone()), // NOT TESTED
            )
        };
        check_added_monitors!($node_b, 1);
        if $fail_backwards {
            assert!($node_a.node.get_and_clear_pending_events().is_empty());
            assert!($node_a.node.get_and_clear_pending_msg_events().is_empty());
        }
        (extra_msg_option, bs_revoke_and_ack)
    }};
    ($node_a: expr, $node_b: expr, $commitment_signed: expr, $fail_backwards: expr, true /* skip last step */, false /* return extra message */, true /* return last RAA */) => {{
        check_added_monitors!($node_a, 0);
        assert!($node_a.node.get_and_clear_pending_msg_events().is_empty());
        $node_a
            .node
            .handle_commitment_signed(&$node_b.node.get_our_node_id(), &$commitment_signed);
        check_added_monitors!($node_a, 1);
        let (extra_msg_option, bs_revoke_and_ack) =
            commitment_signed_dance!($node_a, $node_b, (), $fail_backwards, true, true, true);
        assert!(extra_msg_option.is_none());
        bs_revoke_and_ack
    }};
    ($node_a: expr, $node_b: expr, (), $fail_backwards: expr, true /* skip last step */, true /* return extra message */) => {{
        let (extra_msg_option, bs_revoke_and_ack) =
            commitment_signed_dance!($node_a, $node_b, (), $fail_backwards, true, true, true);
        $node_a
            .node
            .handle_revoke_and_ack(&$node_b.node.get_our_node_id(), &bs_revoke_and_ack);
        check_added_monitors!($node_a, 1);
        extra_msg_option
    }};
    ($node_a: expr, $node_b: expr, (), $fail_backwards: expr, true /* skip last step */, false /* no extra message */) => {{
        assert!(
            commitment_signed_dance!($node_a, $node_b, (), $fail_backwards, true, true).is_none()
        );
    }};
    ($node_a: expr, $node_b: expr, $commitment_signed: expr, $fail_backwards: expr) => {{
        commitment_signed_dance!($node_a, $node_b, $commitment_signed, $fail_backwards, true);
        if $fail_backwards {
            expect_pending_htlcs_forwardable!($node_a);
            check_added_monitors!($node_a, 1);
        } else {
            assert!($node_a.node.get_and_clear_pending_msg_events().is_empty());
        }
    }};
}

macro_rules! get_payment_preimage_hash {
    ($node: expr) => {{
        let payment_preimage = PaymentPreimage([*$node.network_payment_count.borrow(); 32]);
        *$node.network_payment_count.borrow_mut() += 1;
        let payment_hash = PaymentHash(Sha256::hash(&payment_preimage.0[..]).into_inner());
        (payment_preimage, payment_hash)
    }};
}

macro_rules! expect_pending_htlcs_forwardable {
    ($node: expr) => {{
        let events = $node.node.get_and_clear_pending_events();
        assert_eq!(events.len(), 1);
        match events[0] {
            Event::PendingHTLCsForwardable { .. } => {}
            _ => panic!("Unexpected event"),
        };
        $node.node.process_pending_htlc_forwards();
    }};
}

macro_rules! expect_payment_sent {
    ($node: expr, $expected_payment_preimage: expr) => {
        let events = $node.node.get_and_clear_pending_events();
        assert_eq!(events.len(), 1);
        match events[0] {
            Event::PaymentSent {
                ref payment_preimage,
            } => {
                assert_eq!($expected_payment_preimage, *payment_preimage);
            }
            _ => panic!("Unexpected event"),
        }
    };
}

#[macro_export]
macro_rules! expect_pending_htlcs_forwardable_ignore {
	($node: expr) => {{
        use lightning::util::events::Event;
        use lightning::util::events::EventsProvider;
		let events = $node.node.get_and_clear_pending_events();
		assert_eq!(events.len(), 1);
		match events[0] {
			Event::PendingHTLCsForwardable { .. } => { },
			_ => panic!("Unexpected event"),
		};
	}}
}

#[macro_export]
macro_rules! expect_payment_failed {
	($node: expr, $expected_payment_hash: expr, $rejected_by_dest: expr $(, $expected_error_code: expr, $expected_error_data: expr)*) => {
        use lightning::util::events::Event;
        use lightning::util::events::EventsProvider;
		let events = $node.node.get_and_clear_pending_events();
		assert_eq!(events.len(), 1);
		match events[0] {
			Event::PaymentFailed { ref payment_hash, rejected_by_dest, /*ref error_code, ref error_data*/ } => {
				assert_eq!(*payment_hash, $expected_payment_hash, "unexpected payment_hash");
				assert_eq!(rejected_by_dest, $rejected_by_dest, "unexpected rejected_by_dest value");
				// assert!(error_code.is_some(), "expected error_code.is_some() = true");
				// assert!(error_data.is_some(), "expected error_data.is_some() = true");
				// $(
				// 	assert_eq!(error_code.unwrap(), $expected_error_code, "unexpected error code");
				// 	assert_eq!(&error_data.as_ref().unwrap()[..], $expected_error_data, "unexpected error data");
				// )*
			},
			_ => panic!("Unexpected event"),
		}
	}
}

pub fn send_along_route_with_secret<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    route: Route,
    expected_paths: &[&[&Node<'a, 'b, 'c>]],
    recv_value: u64,
    our_payment_hash: PaymentHash,
    our_payment_secret: Option<PaymentSecret>,
) {
    origin_node
        .node
        .send_payment(&route, our_payment_hash, &our_payment_secret)
        .unwrap();
    check_added_monitors!(origin_node, expected_paths.len());
    pass_along_route(
        origin_node,
        expected_paths,
        recv_value,
        our_payment_hash,
        our_payment_secret,
    );
}

pub fn pass_along_path<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    expected_path: &[&Node<'a, 'b, 'c>],
    recv_value: u64,
    our_payment_hash: PaymentHash,
    our_payment_secret: Option<PaymentSecret>,
    ev: MessageSendEvent,
    payment_received_expected: bool,
) {
    let mut payment_event = SendEvent::from_event(ev);
    let mut prev_node = origin_node;

    for (idx, &node) in expected_path.iter().enumerate() {
        assert_eq!(node.node.get_our_node_id(), payment_event.node_id);

        node.node
            .handle_update_add_htlc(&prev_node.node.get_our_node_id(), &payment_event.msgs[0]);
        check_added_monitors!(node, 0);
        commitment_signed_dance!(node, prev_node, payment_event.commitment_msg, false);

        expect_pending_htlcs_forwardable!(node);

        if idx == expected_path.len() - 1 {
            let events_2 = node.node.get_and_clear_pending_events();
            if payment_received_expected {
                assert_eq!(events_2.len(), 1);
                match events_2[0] {
                    Event::PaymentReceived {
                        ref payment_hash,
                        ref payment_secret,
                        amt,
                    } => {
                        assert_eq!(our_payment_hash, *payment_hash);
                        assert_eq!(our_payment_secret, *payment_secret);
                        assert_eq!(amt, recv_value);
                    }
                    _ => panic!("Unexpected event"), // NOT TESTED
                }
            } else {
                assert!(events_2.is_empty()); // NOT TESTED
            }
        } else {
            let mut events_2 = node.node.get_and_clear_pending_msg_events();
            assert_eq!(events_2.len(), 1);
            check_added_monitors!(node, 1);
            payment_event = SendEvent::from_event(events_2.remove(0));
            assert_eq!(payment_event.msgs.len(), 1);
        }

        prev_node = node;
    }
}

pub fn pass_along_route<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    expected_route: &[&[&Node<'a, 'b, 'c>]],
    recv_value: u64,
    our_payment_hash: PaymentHash,
    our_payment_secret: Option<PaymentSecret>,
) {
    let mut events = origin_node.node.get_and_clear_pending_msg_events();
    assert_eq!(events.len(), expected_route.len());
    for (path_idx, (ev, expected_path)) in events.drain(..).zip(expected_route.iter()).enumerate() {
        // Once we've gotten through all the HTLCs, the last one should result in a
        // PaymentReceived (but each previous one should not!), .
        let expect_payment = path_idx == expected_route.len() - 1;
        pass_along_path(
            origin_node,
            expected_path,
            recv_value,
            our_payment_hash.clone(),
            our_payment_secret,
            ev,
            expect_payment,
        );
    }
}

pub fn send_along_route_with_hash<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    route: Route,
    expected_route: &[&Node<'a, 'b, 'c>],
    recv_value: u64,
    our_payment_hash: PaymentHash,
) {
    send_along_route_with_secret(
        origin_node,
        route,
        &[expected_route],
        recv_value,
        our_payment_hash,
        None,
    );
}

pub fn send_along_route<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    route: Route,
    expected_route: &[&Node<'a, 'b, 'c>],
    recv_value: u64,
) -> (PaymentPreimage, PaymentHash) {
    let (our_payment_preimage, our_payment_hash) = get_payment_preimage_hash!(origin_node);
    send_along_route_with_hash(
        origin_node,
        route,
        expected_route,
        recv_value,
        our_payment_hash,
    );
    (our_payment_preimage, our_payment_hash)
}

pub fn claim_payment_along_route_with_secret<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    expected_paths: &[&[&Node<'a, 'b, 'c>]],
    skip_last: bool,
    our_payment_preimage: PaymentPreimage,
    our_payment_secret: Option<PaymentSecret>,
    expected_amount: u64,
) {
    for path in expected_paths.iter() {
        assert_eq!(
            path.last().unwrap().node.get_our_node_id(),
            expected_paths[0].last().unwrap().node.get_our_node_id()
        );
    }
    assert!(expected_paths[0].last().unwrap().node.claim_funds(
        our_payment_preimage,
        &our_payment_secret,
        expected_amount
    ));
    check_added_monitors!(expected_paths[0].last().unwrap(), expected_paths.len());

    macro_rules! msgs_from_ev {
        ($ev: expr) => {
            match $ev {
                &MessageSendEvent::UpdateHTLCs {
                    ref node_id,
                    updates:
                        msgs::CommitmentUpdate {
                            ref update_add_htlcs,
                            ref update_fulfill_htlcs,
                            ref update_fail_htlcs,
                            ref update_fail_malformed_htlcs,
                            ref update_fee,
                            ref commitment_signed,
                        },
                } => {
                    assert!(update_add_htlcs.is_empty());
                    assert_eq!(update_fulfill_htlcs.len(), 1);
                    assert!(update_fail_htlcs.is_empty());
                    assert!(update_fail_malformed_htlcs.is_empty());
                    assert!(update_fee.is_none());
                    (
                        (update_fulfill_htlcs[0].clone(), commitment_signed.clone()),
                        node_id.clone(),
                    )
                }
                _ => panic!("Unexpected event"),
            }
        };
    }
    let mut per_path_msgs: Vec<((msgs::UpdateFulfillHTLC, msgs::CommitmentSigned), PublicKey)> =
        Vec::with_capacity(expected_paths.len());
    let events = expected_paths[0]
        .last()
        .unwrap()
        .node
        .get_and_clear_pending_msg_events();
    assert_eq!(events.len(), expected_paths.len());
    for ev in events.iter() {
        per_path_msgs.push(msgs_from_ev!(ev));
    }

    for (expected_route, (path_msgs, next_hop)) in
        expected_paths.iter().zip(per_path_msgs.drain(..))
    {
        let mut next_msgs = Some(path_msgs);
        let mut expected_next_node = next_hop;

        macro_rules! last_update_fulfill_dance {
            ($node: expr, $prev_node: expr) => {{
                $node.node.handle_update_fulfill_htlc(
                    &$prev_node.node.get_our_node_id(),
                    &next_msgs.as_ref().unwrap().0,
                );
                check_added_monitors!($node, 0);
                assert!($node.node.get_and_clear_pending_msg_events().is_empty());
                commitment_signed_dance!($node, $prev_node, next_msgs.as_ref().unwrap().1, false);
            }};
        }
        macro_rules! mid_update_fulfill_dance {
            ($node: expr, $prev_node: expr, $new_msgs: expr) => {{
                $node.node.handle_update_fulfill_htlc(
                    &$prev_node.node.get_our_node_id(),
                    &next_msgs.as_ref().unwrap().0,
                );
                check_added_monitors!($node, 1);
                let new_next_msgs = if $new_msgs {
                    let events = $node.node.get_and_clear_pending_msg_events();
                    assert_eq!(events.len(), 1);
                    let (res, nexthop) = msgs_from_ev!(&events[0]);
                    expected_next_node = nexthop;
                    Some(res)
                } else {
                    assert!($node.node.get_and_clear_pending_msg_events().is_empty());
                    None
                };
                commitment_signed_dance!($node, $prev_node, next_msgs.as_ref().unwrap().1, false);
                next_msgs = new_next_msgs;
            }};
        }

        let mut prev_node = expected_route.last().unwrap();
        for (idx, node) in expected_route.iter().rev().enumerate().skip(1) {
            assert_eq!(expected_next_node, node.node.get_our_node_id());
            let update_next_msgs = !skip_last || idx != expected_route.len() - 1;
            if next_msgs.is_some() {
                mid_update_fulfill_dance!(node, prev_node, update_next_msgs);
            } else {
                // BEGIN NOT TESTED
                assert!(!update_next_msgs);
                assert!(node.node.get_and_clear_pending_msg_events().is_empty());
                // END NOT TESTED
            }
            if !skip_last && idx == expected_route.len() - 1 {
                assert_eq!(expected_next_node, origin_node.node.get_our_node_id());
            }

            prev_node = node;
        }

        if !skip_last {
            last_update_fulfill_dance!(origin_node, expected_route.first().unwrap());
            expect_payment_sent!(origin_node, our_payment_preimage);
        }
    }
}

pub fn claim_payment_along_route<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    expected_route: &[&Node<'a, 'b, 'c>],
    skip_last: bool,
    our_payment_preimage: PaymentPreimage,
    expected_amount: u64,
) {
    claim_payment_along_route_with_secret(
        origin_node,
        &[expected_route],
        skip_last,
        our_payment_preimage,
        None,
        expected_amount,
    );
}

pub fn claim_payment<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    expected_route: &[&Node<'a, 'b, 'c>],
    our_payment_preimage: PaymentPreimage,
    expected_amount: u64,
) {
    claim_payment_along_route(
        origin_node,
        expected_route,
        false,
        our_payment_preimage,
        expected_amount,
    );
}

pub const TEST_FINAL_CLTV: u32 = 32;

pub fn route_payment<'a, 'b, 'c>(
    origin_node: &Node<'a, 'b, 'c>,
    expected_route: &[&Node<'a, 'b, 'c>],
    recv_value: u64,
) -> (PaymentPreimage, PaymentHash) {
    let net_graph_msg_handler = &origin_node.net_graph_msg_handler;
    let logger = test_utils::TestLogger::new();
    let route = get_route(&origin_node.node.get_our_node_id(), &net_graph_msg_handler.network_graph.read().unwrap(), &expected_route.last().unwrap().node.get_our_node_id(), None, None, &Vec::new(), recv_value, TEST_FINAL_CLTV, &logger).unwrap();
    assert_eq!(route.paths.len(), 1);
    assert_eq!(route.paths[0].len(), expected_route.len());
    for (node, hop) in expected_route.iter().zip(route.paths[0].iter()) {
        assert_eq!(hop.pubkey, node.node.get_our_node_id());
    }

    send_along_route(origin_node, route, expected_route, recv_value)
}

pub fn send_payment<'a, 'b, 'c>(
    origin: &Node<'a, 'b, 'c>,
    expected_route: &[&Node<'a, 'b, 'c>],
    recv_value: u64,
    expected_value: u64,
) {
    let our_payment_preimage = route_payment(&origin, expected_route, recv_value).0;
    claim_payment(
        &origin,
        expected_route,
        our_payment_preimage,
        expected_value,
    );
}

pub fn create_chanmon_cfgs(node_count: usize) -> Vec<TestChanMonCfg> {
    let mut chan_mon_cfgs = Vec::new();
    for i in 0..node_count {
        let tx_broadcaster = test_utils::TestBroadcaster {
            txn_broadcasted: Mutex::new(Vec::new()),
        };
        let fee_estimator = test_utils::TestFeeEstimator { sat_per_kw: 253 };
        let chain_source = test_utils::TestChainSource::new(Network::Testnet);
        let logger = test_utils::TestLogger::with_id(format!("node {}", i));
        let persister = TestPersister::new();
        chan_mon_cfgs.push(TestChanMonCfg {
            tx_broadcaster,
            fee_estimator,
            chain_source,
            logger,
            persister,
        });
    }

    chan_mon_cfgs
}

pub fn create_node_chanmgrs<'a, 'b>(
    node_count: usize,
    cfgs: &'a Vec<NodeCfg<'b>>,
    node_config: &[Option<UserConfig>],
) -> Vec<
    ChannelManager<
        LoopbackChannelSigner,
        &'a TestChainMonitor<'b>,
        &'b test_utils::TestBroadcaster,
        &'a LoopbackSignerKeysInterface,
        &'b test_utils::TestFeeEstimator,
        &'b test_utils::TestLogger,
    >,
> {
    let mut chanmgrs = Vec::new();
    for i in 0..node_count {
        let mut default_config = UserConfig::default();
        default_config.channel_options.announced_channel = true;
        default_config
            .peer_channel_config_limits
            .force_announced_channel_preference = false;
        default_config.own_channel_config.our_htlc_minimum_msat = 1000; // sanitization being done by the sender, to exerce receiver logic we need to lift of limit
        let network = Network::Testnet;
        let params = ChainParameters {
            network,
            latest_hash: genesis_block(network).header.block_hash(),
            latest_height: 0,
        };
        let node = ChannelManager::new(cfgs[i].fee_estimator, &cfgs[i].chain_monitor, cfgs[i].tx_broadcaster, cfgs[i].logger, &cfgs[i].keys_manager, if node_config[i].is_some() { node_config[i].clone().unwrap() } else { default_config }, params);
        chanmgrs.push(node);
    }

    chanmgrs
}

pub fn create_network<'a, 'b: 'a, 'c: 'b>(
    node_count: usize,
    cfgs: &'b Vec<NodeCfg<'c>>,
    chan_mgrs: &'a Vec<
        ChannelManager<
            LoopbackChannelSigner,
            &'b TestChainMonitor<'c>,
            &'c test_utils::TestBroadcaster,
            &'b LoopbackSignerKeysInterface,
            &'c test_utils::TestFeeEstimator,
            &'c test_utils::TestLogger,
        >,
    >,
) -> Vec<Node<'a, 'b, 'c>> {
    let mut nodes = Vec::new();
    let chan_count = Rc::new(RefCell::new(0));
    let payment_count = Rc::new(RefCell::new(0));

    for i in 0..node_count {
        let net_graph_msg_handler =
            NetGraphMsgHandler::new(cfgs[i].chain_source.genesis_hash, None, cfgs[i].logger);
        let connect_style = Rc::new(RefCell::new(ConnectStyle::FullBlockViaListen));
        nodes.push(Node {
            chain_source: cfgs[i].chain_source,
            tx_broadcaster: cfgs[i].tx_broadcaster,
            chain_monitor: &cfgs[i].chain_monitor,
            keys_manager: &cfgs[i].keys_manager,
            node: &chan_mgrs[i],
            net_graph_msg_handler,
            node_seed: cfgs[i].node_seed,
            network_chan_count: chan_count.clone(),
            network_payment_count: payment_count.clone(),
            logger: cfgs[i].logger,
            blocks: RefCell::new(vec![(genesis_block(Network::Testnet).header, 0)]),
            connect_style: Rc::clone(&connect_style),
        })
    }

    nodes
}

// BEGIN NOT TESTED
pub fn dump_node_txn(prefix: &str, node: &Node) {
    let node_txn = node.tx_broadcaster.txn_broadcasted.lock().unwrap();
    dump_txn(prefix, &*node_txn);
}

pub fn dump_txn(prefix: &str, txn: &Vec<Transaction>) {
    println!("{}", prefix);
    for x in txn {
        println!("{} {} {:?}", prefix, x.txid(), x);
    }
}
// END NOT TESTED

pub fn get_announce_close_broadcast_events<'a, 'b, 'c>(nodes: &Vec<Node<'a, 'b, 'c>>, a: usize, b: usize)  {
    let events_1 = nodes[a].node.get_and_clear_pending_msg_events();
    assert_eq!(events_1.len(), 2);
    let as_update = match events_1[0] {
        MessageSendEvent::BroadcastChannelUpdate { ref msg } => {
            msg.clone()
        },
        _ => panic!("Unexpected event"),
    };
    match events_1[1] {
        MessageSendEvent::HandleError { node_id, action: msgs::ErrorAction::SendErrorMessage { ref msg } } => {
            assert_eq!(node_id, nodes[b].node.get_our_node_id());
            assert_eq!(msg.data, "Commitment or closing transaction was confirmed on chain.");
        },
        _ => panic!("Unexpected event"),
    }

    let events_2 = nodes[b].node.get_and_clear_pending_msg_events();
    assert_eq!(events_2.len(), 2);
    let bs_update = match events_2[0] {
        MessageSendEvent::BroadcastChannelUpdate { ref msg } => {
            msg.clone()
        },
        _ => panic!("Unexpected event"),
    };
    match events_2[1] {
        MessageSendEvent::HandleError { node_id, action: msgs::ErrorAction::SendErrorMessage { ref msg } } => {
            assert_eq!(node_id, nodes[a].node.get_our_node_id());
            assert_eq!(msg.data, "Commitment or closing transaction was confirmed on chain.");
        },
        _ => panic!("Unexpected event"),
    }

    for node in nodes {
        node.net_graph_msg_handler.handle_channel_update(&as_update).unwrap();
        node.net_graph_msg_handler.handle_channel_update(&bs_update).unwrap();
    }
}