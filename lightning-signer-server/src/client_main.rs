extern crate clap;

use std::io;

use clap::{Command, Arg, ArgMatches};

use bip39::Mnemonic;
use lightning_signer_server::client::driver;
use lightning_signer_server::CLIENT_APP_NAME;
use lightning_signer_server::NETWORK_NAMES;

fn make_test_subapp() -> Command<'static> {
    Command::new("test").before_help("run a test scenario").subcommand(Command::new("integration"))
}

#[tokio::main]
async fn test_subcommand(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = driver::connect().await?;

    match matches.subcommand() {
        Some(("integration", _)) => driver::integration_test(&mut client).await?,
        Some((name, _)) => panic!("unimplemented command {}", name),
        None => {
            println!("missing sub-command");
            make_test_subapp().print_help()?
        }
    };
    Ok(())
}

#[tokio::main]
async fn ping_subcommand() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = driver::connect().await?;
    driver::ping(&mut client).await
}

fn make_node_subapp() -> Command<'static> {
    Command::new("node")
        .before_help("control a node")
        .subcommand(
            Command::new("new")
                .before_help("Add a new node to the signer.  Outputs the node ID to stdout and the mnemonic to stderr.")
                .arg(Arg::new("mnemonic")
                     .help("read mnemonic from stdin")
                     .long("mnemonic")
                     .short('m')
                     .takes_value(false))
                .arg(Arg::new("network")
                     .help("network name")
                     .long("network")
                     .takes_value(true)
                     .possible_values(&NETWORK_NAMES)
                     .default_value(NETWORK_NAMES[0]),
                )
        )
        .subcommand(Command::new("list").before_help("List configured nodes."))
}

#[tokio::main]
async fn node_subcommand(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = driver::connect().await?;

    match matches.subcommand() {
        Some(("new", matches)) => {
            let network_name = matches.value_of_t("network").expect("network");
            if matches.is_present("mnemonic") {
                let mut buf = String::new();
                io::stdin().read_line(&mut buf).expect("stdin");
                let mnemonic = Mnemonic::parse(buf.trim())?;
                driver::new_node_with_mnemonic(&mut client, mnemonic, network_name).await?
            } else {
                driver::new_node(&mut client, network_name).await?
            }
        }
        Some(("list", _)) => driver::list_nodes(&mut client).await?,
        Some((name, _)) => panic!("unimplemented command {}", name),
        None => {
            println!("missing sub-command");
            make_node_subapp().print_help()?
        }
    };
    Ok(())
}

fn make_chan_subapp() -> Command<'static> {
    Command::new("channel")
        .alias("chan")
        .before_help("control a channel")
        .subcommand(
            Command::new("new")
                .before_help("Add a new channel to a node.  Outputs the channel ID.")
                .arg(
                    Arg::new("no-nonce")
                        .help("generate the nonce on the server")
                        .long("no-nonce")
                        .takes_value(false),
                )
                .arg(
                    Arg::new("nonce")
                        .takes_value(true)
                        .help("optional nonce, otherwise one will be generated and displayed"),
                ),
        )
        .subcommand(Command::new("list").before_help("List channels in a node"))
}

#[tokio::main]
async fn chan_subcommand(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = driver::connect().await?;
    // TODO give a nice error message if node_id is missing
    let node_id = hex::decode(matches.value_of("node").expect("missing node_id"))?;

    match matches.subcommand() {
        Some(("new", matches)) =>
            driver::new_channel(
                &mut client,
                node_id,
                matches.value_of("nonce"),
                matches.is_present("no-nonce"),
            )
            .await?,
        Some(("list", _)) => driver::list_channels(&mut client, node_id).await?,
        Some((name, _)) => panic!("unimplemented command {}", name),
        None => {
            println!("missing sub-command");
            make_chan_subapp().print_help()?
        }
    };
    Ok(())
}

fn make_allowlist_subapp() -> Command<'static> {
    Command::new("allowlist")
        .alias("alst")
        .before_help("manage allowlists")
        .subcommand(Command::new("list").before_help("List allowlisted addresses for a node"))
        .subcommand(
            Command::new("add").before_help("Add address to the node's allowlist").arg(
                Arg::new("address")
                    .takes_value(true)
                    .required(true)
                    .help("address to add to the allowlist"),
            ),
        )
        .subcommand(
            Command::new("remove").before_help("Remove address from the node's allowlist").arg(
                Arg::new("address")
                    .takes_value(true)
                    .required(true)
                    .help("address to remove from the allowlist"),
            ),
        )
}

#[tokio::main]
async fn alst_subcommand(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = driver::connect().await?;
    // TODO give a nice error message if node_id is missing
    let node_id = hex::decode(matches.value_of("node").expect("missing node_id"))?;

    match matches.subcommand() {
        Some(("list", _)) => driver::list_allowlist(&mut client, node_id).await?,
        Some(("add", matches)) => {
            let addrs = vec![matches.value_of("address").expect("missing address").to_string()];
            driver::add_allowlist(&mut client, node_id, addrs).await?
        }
        Some(("remove", matches)) => {
            let addrs = vec![matches.value_of("address").expect("missing address").to_string()];
            driver::remove_allowlist(&mut client, node_id, addrs).await?
        }
        Some((name, _)) => panic!("unimplemented command {}", name),
        None => {
            println!("missing sub-command");
            make_allowlist_subapp().print_help()?
        }
    };
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_subapp = make_test_subapp();
    let node_subapp = make_node_subapp();
    let chan_subapp = make_chan_subapp();
    let alst_subapp = make_allowlist_subapp();
    let app = Command::new(CLIENT_APP_NAME)
        .before_help("a CLI utility which communicates with a running Validating Lightning Signer server via gRPC")
        .arg(
            Arg::new("node")
                .short('n')
                .long("node")
                .takes_value(true)
                .global(true)
                .validator(|v| hex::decode(v)),
        )
        .subcommand(test_subapp)
        .subcommand(node_subapp)
        .subcommand(chan_subapp)
        .subcommand(alst_subapp)
        .subcommand(Command::new("ping"));
    let matches = app.clone().get_matches();

    match matches.subcommand() {
        Some(("test", submatches)) => test_subcommand(submatches)?,
        Some(("ping", _)) => ping_subcommand()?,
        Some(("node", submatches)) => node_subcommand(submatches)?,
        Some(("channel", submatches)) => chan_subcommand(submatches)?,
        Some(("allowlist", submatches)) => alst_subcommand(submatches)?,
        Some((name, _)) => panic!("unimplemented command {}", name),
        None => panic!("unmatched command?!"),
    };
    Ok(())
}
