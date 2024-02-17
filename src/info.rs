use crate::config::Configuration;

const SEPARATOR: &str = "\r\n";
const SECTIONS: &[(&str, &str)] = &[
    ("replication", "Replication"),
];

pub fn info_on(config: &Configuration, section: &str) -> String {
    if section == "replication" {
        let is_replica = config.get("replicaof").is_some();
        let repl_info = config.replica_info();

        vec![
            String::from("# Replication"),
            String::from(if !is_replica { "role:master" } else { "role:slave" }),
            String::from("connected_slaves:0"),
            format!("master_replid:{}", repl_info.digest_string()),
            format!("master_repl_offset:{}", repl_info.offset()),
        ]
    } else {
        vec![]
    }.join(SEPARATOR)
}

pub fn all_info(config: &Configuration) -> String {
    let mut tmp: Vec<String> = vec![];

    for &(key, _name) in SECTIONS.iter() {
        tmp.push(info_on(config, key));
    }

    tmp.join(&SEPARATOR)
}
