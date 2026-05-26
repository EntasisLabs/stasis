use grapheme_sdk::{
    ExecutableReflectionKind, ModuleSearchDetail, ModuleSearchOptions,
    executables_reflection_contract_from_source, modules_info_contract,
    modules_search_contract, modules_types_contract,
};

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::runtime::workflow_reflection::{
    WorkflowModuleOperationArgReflection, WorkflowModuleOperationObjectFieldReflection,
    WorkflowModuleOperationObjectTypeReflection,
    WorkflowExecutableKind, WorkflowExecutableReflection, WorkflowModuleInfoReflection,
    WorkflowModuleOperationReflection, WorkflowModuleSearchMatchReflection,
    WorkflowModuleSearchReflection, WorkflowModuleTypesReflection, WorkflowReflectionPort,
    WorkflowSourceReflection,
};

#[derive(Clone, Default)]
pub struct GraphemeSdkWorkflowReflection;

impl GraphemeSdkWorkflowReflection {
    pub fn new() -> Self {
        Self
    }

    fn map_kind(kind: ExecutableReflectionKind) -> WorkflowExecutableKind {
        match kind {
            ExecutableReflectionKind::Query => WorkflowExecutableKind::Query,
            ExecutableReflectionKind::Mutation => WorkflowExecutableKind::Mutation,
            ExecutableReflectionKind::Subscription => WorkflowExecutableKind::Subscription,
            ExecutableReflectionKind::Iterator => WorkflowExecutableKind::Iterator,
        }
    }

    fn map_effect(effect: impl std::fmt::Debug) -> String {
        format!("{effect:?}")
    }

    fn map_object_type(
        object_type: Option<grapheme_sdk::OperationObjectType>,
    ) -> Option<WorkflowModuleOperationObjectTypeReflection> {
        object_type.map(|object_type| WorkflowModuleOperationObjectTypeReflection {
            kind: object_type.kind,
            required: object_type.required,
            properties: object_type
                .properties
                .into_iter()
                .map(|(name, field)| {
                    (
                        name,
                        WorkflowModuleOperationObjectFieldReflection {
                            ty: field.ty,
                            required: field.required,
                        },
                    )
                })
                .collect(),
        })
    }

    fn map_operation(op: grapheme_sdk::CompactModuleOp) -> WorkflowModuleOperationReflection {
        WorkflowModuleOperationReflection {
            op: op.op,
            stability: op.stability,
            effect: Self::map_effect(op.effect),
            args: op
                .args
                .into_iter()
                .map(|arg| WorkflowModuleOperationArgReflection {
                    name: arg.name,
                    ty: arg.ty,
                    required: arg.required,
                })
                .collect(),
            input_object_type: Self::map_object_type(op.input_object_type),
            output_object_type: Self::map_object_type(op.output_object_type),
            input_schema_ref: op.input_schema_ref,
            output_schema_ref: op.output_schema_ref,
        }
    }
}

impl WorkflowReflectionPort for GraphemeSdkWorkflowReflection {
    fn reflect_executables_from_source(&self, source: &str) -> Result<WorkflowSourceReflection> {
        let payload = executables_reflection_contract_from_source(source).map_err(|err| {
            StasisError::PortFailure(format!(
                "grapheme executable reflection from source failed: {err}"
            ))
        })?;

        let executables = payload
            .executables
            .into_iter()
            .map(|item| WorkflowExecutableReflection {
                name: item.name,
                kind: Self::map_kind(item.kind),
                input_type: item.input_type,
                output_type: item.output_type,
                loop_directive_count: item.loop_directive_count,
                recursive_directive_count: item.recursive_directive_count,
                retry_directive_count: item.retry_directive_count,
                timeout_directive_count: item.timeout_directive_count,
                pipeline_count: item.pipeline_count,
                step_count: item.step_count,
            })
            .collect::<Vec<_>>();

        Ok(WorkflowSourceReflection {
            count: payload.count,
            executables,
        })
    }

    fn modules_search(&self, query: &str) -> Result<WorkflowModuleSearchReflection> {
        let options = ModuleSearchOptions {
            explain: true,
            detail: ModuleSearchDetail::Full,
            top: Some(25),
            min_score: None,
            include_experimental: false,
        };
        let payload = modules_search_contract(query, &options);
        let matches = payload
            .matches
            .into_iter()
            .map(|row| WorkflowModuleSearchMatchReflection {
                module_id: row.module_id,
                score: row.score,
                summary: row.summary,
                matching_ops: row.matching_ops.unwrap_or_default(),
                related_examples: row.related_examples,
            })
            .collect::<Vec<_>>();

        Ok(WorkflowModuleSearchReflection {
            query: payload.query,
            count: payload.count,
            matches,
        })
    }

    fn module_info(&self, module_id: &str) -> Result<Option<WorkflowModuleInfoReflection>> {
        let Some(payload) = modules_info_contract(module_id) else {
            return Ok(None);
        };

        let exported_ops = payload
            .exported_ops
            .into_iter()
            .map(Self::map_operation)
            .collect::<Vec<_>>();

        Ok(Some(WorkflowModuleInfoReflection {
            module_id: payload.module_id,
            version: payload.version,
            entrypoint: payload.entrypoint,
            required_capabilities: payload.required_capabilities,
            total_ops: payload.op_summary.total_ops,
            exported_ops,
        }))
    }

    fn module_types(&self, module_id: &str) -> Result<Option<WorkflowModuleTypesReflection>> {
        let Some(payload) = modules_types_contract(module_id) else {
            return Ok(None);
        };

        let types = payload
            .types
            .into_iter()
            .map(Self::map_operation)
            .collect::<Vec<_>>();

        Ok(Some(WorkflowModuleTypesReflection {
            module_id: payload.module_id,
            total_types: payload.type_summary.total_ops,
            types,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::GraphemeSdkWorkflowReflection;
    use crate::ports::outbound::runtime::workflow_reflection::{
        WorkflowExecutableKind, WorkflowReflectionPort,
    };

    #[test]
    fn reflects_executable_metadata_from_valid_source() {
        let adapter = GraphemeSdkWorkflowReflection::new();
        let source = r#"
import core from "grapheme/core"

query Echo {
  core.echo(message: "ping") {
    state {
      current
    }
  }
}
"#;

        let reflection = adapter
            .reflect_executables_from_source(source)
            .expect("valid source should reflect");

        assert_eq!(reflection.count, 1);
        assert_eq!(reflection.executables.len(), 1);
        assert_eq!(reflection.executables[0].name, "Echo");
        assert_eq!(reflection.executables[0].kind, WorkflowExecutableKind::Query);
        assert!(reflection.executables[0].pipeline_count >= 1);
    }

    #[test]
    fn returns_port_failure_for_invalid_source() {
        let adapter = GraphemeSdkWorkflowReflection::new();
        let source = "query Broken {";

        let err = adapter
            .reflect_executables_from_source(source)
            .expect_err("invalid source should fail reflection");

        assert!(
            err.to_string()
                .contains("grapheme executable reflection from source failed")
        );
    }

    #[test]
    fn returns_module_search_rows_for_core_query() {
        let adapter = GraphemeSdkWorkflowReflection::new();

        let payload = adapter
            .modules_search("core")
            .expect("module search should succeed");

        assert!(!payload.matches.is_empty());
        assert!(payload
            .matches
            .iter()
            .any(|row| row.module_id == "core"));
    }

    #[test]
    fn returns_module_info_and_types_for_known_module() {
        let adapter = GraphemeSdkWorkflowReflection::new();

        let info = adapter
            .module_info("core")
            .expect("module info should succeed")
            .expect("core module should exist");
        let types = adapter
            .module_types("core")
            .expect("module types should succeed")
            .expect("core module types should exist");

        assert_eq!(info.module_id, "core");
        assert_eq!(types.module_id, "core");
        assert!(info.total_ops > 0);
        assert!(types.total_types > 0);
    }
}
