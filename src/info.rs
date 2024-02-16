use std::collections::HashMap;

const SEPARATOR: &str = "\r\n";
const SECTIONS: &[(&str, &str)] = &[
    ("replication", "Replication"),
];

pub fn info_on(config: &HashMap::<String, String>, section: &str) -> String {
    if section == "replication" {
        let is_replica = config.contains_key("replicaof");

        vec![
            "# Replication",
            if !is_replica { "role:master" } else { "role:slave" },
            "connected_slaves:0",
        ]
    } else {
        vec![]
    }.join(SEPARATOR)
    .to_owned()
}

pub fn all_info(config: &HashMap::<String, String>) -> String {
    let mut tmp: Vec<String> = vec![];

    for &(key, _name) in SECTIONS.iter() {
        tmp.push(info_on(config, key));
    }

    tmp.join(&SEPARATOR)
}
