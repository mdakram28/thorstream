use std::collections::{HashMap, HashSet};
use std::fs;

use tempfile::TempDir;
use thorstream::security::{
    AclEntry, AclOperation, AclPermission, AclResourceType, SaslMechanism, SecurityConfig,
    SecurityManager,
};

#[test]
fn sasl_plain_scram_oauth_authenticate() {
    let mut plain = HashMap::new();
    plain.insert("alice".to_string(), "secret".to_string());

    let mut scram = HashMap::new();
    scram.insert("bob".to_string(), "scram-secret".to_string());

    let mut oauth = HashSet::new();
    oauth.insert("token-123".to_string());

    let mgr = SecurityManager::new(SecurityConfig {
        plain_users: plain,
        scram_users: scram,
        oauth_tokens: oauth,
        ..SecurityConfig::default()
    });

    assert!(mgr
        .authenticate(SaslMechanism::Plain, Some("alice"), "secret")
        .is_ok());
    assert!(mgr
        .authenticate(SaslMechanism::ScramSha256, Some("bob"), "scram-secret")
        .is_ok());
    assert!(mgr
        .authenticate(SaslMechanism::ScramSha512, Some("bob"), "scram-secret")
        .is_ok());
    assert!(mgr
        .authenticate(SaslMechanism::OauthBearer, Some("svc"), "token-123")
        .is_ok());

    assert!(mgr
        .authenticate(SaslMechanism::Plain, Some("alice"), "wrong")
        .is_err());
    assert!(mgr
        .authenticate(SaslMechanism::OauthBearer, Some("svc"), "wrong-token")
        .is_err());
}

#[test]
fn kafka_acl_model_and_rbac_work() {
    let cfg = SecurityConfig {
        acl_default_allow: false,
        acl_entries: vec![AclEntry {
            principal: "alice".to_string(),
            operation: AclOperation::Write,
            resource_type: AclResourceType::Topic,
            resource_pattern: "payments-*".to_string(),
            permission: AclPermission::Allow,
        }],
        rbac_bindings: HashMap::from([("ops".to_string(), vec!["viewer".to_string()])]),
        ..SecurityConfig::default()
    };

    let mgr = SecurityManager::new(cfg);

    assert!(mgr
        .authorize(
            "alice",
            AclOperation::Write,
            AclResourceType::Topic,
            "payments-eu"
        )
        .is_ok());
    assert!(mgr
        .authorize(
            "alice",
            AclOperation::Read,
            AclResourceType::Topic,
            "payments-eu"
        )
        .is_err());

    assert!(mgr
        .authorize(
            "ops",
            AclOperation::Read,
            AclResourceType::Topic,
            "any-topic"
        )
        .is_ok());
    assert!(mgr
        .authorize(
            "ops",
            AclOperation::Write,
            AclResourceType::Topic,
            "any-topic"
        )
        .is_err());
}

#[test]
fn audit_logs_are_written() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.log");

    let mgr = SecurityManager::new(SecurityConfig {
        acl_default_allow: false,
        audit_log_path: Some(audit_path.clone()),
        acl_entries: vec![AclEntry {
            principal: "alice".to_string(),
            operation: AclOperation::Read,
            resource_type: AclResourceType::Topic,
            resource_pattern: "topic-a".to_string(),
            permission: AclPermission::Allow,
        }],
        ..SecurityConfig::default()
    });

    let _ = mgr.authorize_and_audit(
        "alice",
        AclOperation::Read,
        AclResourceType::Topic,
        "topic-a",
    );
    let _ = mgr.authorize_and_audit(
        "alice",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-a",
    );

    let content = fs::read_to_string(audit_path).unwrap();
    assert!(content.contains("\"principal\":\"alice\""));
    assert!(content.contains("\"allowed\":true"));
    assert!(content.contains("\"allowed\":false"));
}
