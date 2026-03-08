use bifrore_embed::provision::{generate_bucketed_client_ids, persist_client_ids};

fn usage() -> ! {
    eprintln!("Usage: bifrore-clientid-provision <user_id> <node_id> <client_count> <output_path>");
    std::process::exit(2);
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        usage();
    }

    let user_id = &args[1];
    let node_id = &args[2];
    let client_count = args[3].parse::<usize>().unwrap_or_else(|_| usage());
    let output_path = &args[4];

    let client_ids = generate_bucketed_client_ids(user_id, node_id, client_count);
    if let Err(err) = persist_client_ids(output_path, &client_ids) {
        eprintln!("failed to persist client ids: {err}");
        std::process::exit(1);
    }
}
