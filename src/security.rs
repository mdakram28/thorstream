use crate::error::{Result, ThorstreamError};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OauthBearer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclOperation {
    Read,
    Write,
    Describe,
    Alter,
    ClusterAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclResourceType {
    Topic,
    Group,
    Cluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPermission {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
pub struct AclEntry {
    pub principal: String,
    pub operation: AclOperation,
    pub resource_type: AclResourceType,
    pub resource_pattern: String,
    pub permission: AclPermission,
}

#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub plain_users: HashMap<String, String>,
    pub scram_users: HashMap<String, String>,
    pub oauth_tokens: HashSet<String>,
    pub acl_entries: Vec<AclEntry>,
    pub acl_default_allow: bool,
    pub rbac_bindings: HashMap<String, Vec<String>>,
    pub audit_log_path: Option<PathBuf>,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            plain_users: HashMap::new(),
            scram_users: HashMap::new(),
            oauth_tokens: HashSet::new(),
            acl_entries: Vec::new(),
            acl_default_allow: true,
            rbac_bindings: HashMap::new(),
            audit_log_path: None,
        }
    }
}

pub struct SecurityManager {
    cfg: SecurityConfig,
}

#[derive(Debug, Serialize)]
struct AuditEvent {
    ts_unix_ms: u128,
    principal: String,
    operation: String,
    resource_type: String,
    resource: String,
    allowed: bool,
    reason: String,
}

impl SecurityManager {
    pub fn from_env() -> Self {
        let mut cfg = SecurityConfig::default();
        cfg.plain_users = parse_user_map("THORSTREAM_SASL_PLAIN_USERS");
        cfg.scram_users = parse_user_map("THORSTREAM_SASL_SCRAM_USERS");
        cfg.oauth_tokens = parse_token_set("THORSTREAM_SASL_OAUTH_TOKENS");
        cfg.acl_entries = parse_acl_entries("THORSTREAM_ACL_RULES");
        cfg.acl_default_allow = std::env::var("THORSTREAM_ACL_DEFAULT_ALLOW")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(cfg.acl_entries.is_empty());
        cfg.rbac_bindings = parse_rbac_bindings("THORSTREAM_RBAC_BINDINGS");
        cfg.audit_log_path = std::env::var("THORSTREAM_AUDIT_LOG_PATH")
            .ok()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from);
        Self { cfg }
    }

    pub fn new(cfg: SecurityConfig) -> Self {
        Self { cfg }
    }

    pub fn authenticate(
        &self,
        mechanism: SaslMechanism,
        username: Option<&str>,
        secret: &str,
    ) -> Result<String> {
        match mechanism {
            SaslMechanism::Plain => {
                let user = username.unwrap_or_default();
                if let Some(expected) = self.cfg.plain_users.get(user) {
                    if expected == secret {
                        return Ok(user.to_string());
                    }
                }
                Err(ThorstreamError::Unauthorized(
                    "SASL/PLAIN authentication failed".to_string(),
                ))
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                let user = username.unwrap_or_default();
                if let Some(expected) = self.cfg.scram_users.get(user) {
                    if expected == secret {
                        return Ok(user.to_string());
                    }
                }
                Err(ThorstreamError::Unauthorized(
                    "SASL/SCRAM authentication failed".to_string(),
                ))
            }
            SaslMechanism::OauthBearer => {
                if self.cfg.oauth_tokens.contains(secret) {
                    return Ok(username.unwrap_or("oauth-user").to_string());
                }
                Err(ThorstreamError::Unauthorized(
                    "SASL/OAUTHBEARER token rejected".to_string(),
                ))
            }
        }
    }

    pub fn authorize(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> Result<()> {
        let mut matched = false;
        let mut allowed = self.cfg.acl_default_allow;

        let role_entries = role_acl_entries(principal, &self.cfg.rbac_bindings);

        for entry in self.cfg.acl_entries.iter().chain(role_entries.iter()) {
            if !principal_match(&entry.principal, principal) {
                continue;
            }
            if entry.operation != operation || entry.resource_type != resource_type {
                continue;
            }
            if !resource_match(&entry.resource_pattern, resource_name) {
                continue;
            }
            matched = true;
            allowed = entry.permission == AclPermission::Allow;
        }

        if matched {
            if allowed {
                Ok(())
            } else {
                Err(ThorstreamError::Unauthorized(format!(
                    "ACL denied {:?} on {:?}/{} for principal {}",
                    operation, resource_type, resource_name, principal
                )))
            }
        } else if self.cfg.acl_default_allow {
            Ok(())
        } else {
            Err(ThorstreamError::Unauthorized(format!(
                "ACL default deny for {:?} on {:?}/{} for principal {}",
                operation, resource_type, resource_name, principal
            )))
        }
    }

    pub fn authorize_and_audit(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> Result<()> {
        let result = self.authorize(principal, operation, resource_type, resource_name);
        let reason = match &result {
            Ok(_) => "ok".to_string(),
            Err(e) => e.to_string(),
        };
        self.audit(
            principal,
            operation,
            resource_type,
            resource_name,
            result.is_ok(),
            &reason,
        );
        result
    }

    fn audit(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
        allowed: bool,
        reason: &str,
    ) {
        let Some(path) = &self.cfg.audit_log_path else {
            return;
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        let event = AuditEvent {
            ts_unix_ms: now,
            principal: principal.to_string(),
            operation: format!("{:?}", operation),
            resource_type: format!("{:?}", resource_type),
            resource: resource_name.to_string(),
            allowed,
            reason: reason.to_string(),
        };

        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            if let Ok(line) = serde_json::to_string(&event) {
                let _ = writeln!(file, "{}", line);
            }
        }
    }
}

fn parse_user_map(key: &str) -> HashMap<String, String> {
    let raw = std::env::var(key).unwrap_or_default();
    let mut out = HashMap::new();
    for pair in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let mut parts = pair.splitn(2, ':');
        let user = parts.next().unwrap_or_default().trim();
        let pass = parts.next().unwrap_or_default().trim();
        if !user.is_empty() {
            out.insert(user.to_string(), pass.to_string());
        }
    }
    out
}

fn parse_token_set(key: &str) -> HashSet<String> {
    std::env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn parse_acl_entries(key: &str) -> Vec<AclEntry> {
    let raw = std::env::var(key).unwrap_or_default();
    let mut out = Vec::new();
    for row in raw.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = row.split('|').collect();
        if parts.len() != 5 {
            continue;
        }
        let Some(op) = parse_operation(parts[1]) else {
            continue;
        };
        let Some(rt) = parse_resource_type(parts[2]) else {
            continue;
        };
        let Some(permission) = parse_permission(parts[4]) else {
            continue;
        };
        out.push(AclEntry {
            principal: parts[0].trim().to_string(),
            operation: op,
            resource_type: rt,
            resource_pattern: parts[3].trim().to_string(),
            permission,
        });
    }
    out
}

fn parse_rbac_bindings(key: &str) -> HashMap<String, Vec<String>> {
    let raw = std::env::var(key).unwrap_or_default();
    let mut out = HashMap::new();
    for row in raw.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let mut parts = row.splitn(2, '=');
        let principal = parts.next().unwrap_or_default().trim();
        let roles = parts.next().unwrap_or_default();
        if principal.is_empty() {
            continue;
        }
        let parsed_roles = roles
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        out.insert(principal.to_string(), parsed_roles);
    }
    out
}

fn role_acl_entries(principal: &str, bindings: &HashMap<String, Vec<String>>) -> Vec<AclEntry> {
    let roles = bindings.get(principal).cloned().unwrap_or_default();
    let mut out = Vec::new();
    for role in roles {
        match role.as_str() {
            "admin" => {
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::ClusterAction,
                    resource_type: AclResourceType::Cluster,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Write,
                    resource_type: AclResourceType::Topic,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Read,
                    resource_type: AclResourceType::Topic,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Describe,
                    resource_type: AclResourceType::Cluster,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
            }
            "developer" => {
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Write,
                    resource_type: AclResourceType::Topic,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Read,
                    resource_type: AclResourceType::Topic,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Describe,
                    resource_type: AclResourceType::Cluster,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
            }
            "viewer" => {
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Read,
                    resource_type: AclResourceType::Topic,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
                out.push(AclEntry {
                    principal: principal.to_string(),
                    operation: AclOperation::Describe,
                    resource_type: AclResourceType::Cluster,
                    resource_pattern: "*".to_string(),
                    permission: AclPermission::Allow,
                });
            }
            _ => {}
        }
    }
    out
}

fn parse_operation(v: &str) -> Option<AclOperation> {
    match v.trim().to_uppercase().as_str() {
        "READ" => Some(AclOperation::Read),
        "WRITE" => Some(AclOperation::Write),
        "DESCRIBE" => Some(AclOperation::Describe),
        "ALTER" => Some(AclOperation::Alter),
        "CLUSTER_ACTION" => Some(AclOperation::ClusterAction),
        _ => None,
    }
}

fn parse_resource_type(v: &str) -> Option<AclResourceType> {
    match v.trim().to_uppercase().as_str() {
        "TOPIC" => Some(AclResourceType::Topic),
        "GROUP" => Some(AclResourceType::Group),
        "CLUSTER" => Some(AclResourceType::Cluster),
        _ => None,
    }
}

fn parse_permission(v: &str) -> Option<AclPermission> {
    match v.trim().to_uppercase().as_str() {
        "ALLOW" => Some(AclPermission::Allow),
        "DENY" => Some(AclPermission::Deny),
        _ => None,
    }
}

fn principal_match(pattern: &str, principal: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    pattern == principal
}

fn resource_match(pattern: &str, resource: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return resource.starts_with(prefix);
    }
    pattern == resource
}

static SECURITY: OnceLock<SecurityManager> = OnceLock::new();

pub fn security() -> &'static SecurityManager {
    SECURITY.get_or_init(SecurityManager::from_env)
}

pub fn default_principal() -> String {
    std::env::var("THORSTREAM_DEFAULT_PRINCIPAL").unwrap_or_else(|_| "anonymous".to_string())
}
