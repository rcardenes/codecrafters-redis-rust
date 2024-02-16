const SEPARATOR: &str = "\r\n";
const SECTIONS: &[(&str, &str)] = &[
    ("replication", "Replication"),
];

pub fn info_on(section: &str) -> String {
    if section == "replication" {
        vec![
            "# Replication",
            "role:master",
            "connected_slaves:0",
        ]
    } else {
        vec![]
    }.join(SEPARATOR)
    .to_owned()
}

pub fn all_info() -> String {
    let mut tmp: Vec<String> = vec![];

    for &(key, _name) in SECTIONS.iter() {
        tmp.push(info_on(key));
    }

    tmp.join(&SEPARATOR)
}
