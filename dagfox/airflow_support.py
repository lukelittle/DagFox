"""Airflow-specific glue: building the pipeline inside a task and the approval operator.

Everything else in the dagfox package is Airflow-agnostic.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Param

from dagfox.config import Settings, resolve_db_url
from dagfox.db import get_engine
from dagfox.llm.adapter import build_model
from dagfox.notify import build_notifier
from dagfox.pipeline import EntityPipeline, RunContext
from dagfox.policy.engine import PolicyEngine


def build_pipeline() -> EntityPipeline:
    settings = Settings.from_env()
    return EntityPipeline(
        engine=get_engine(resolve_db_url()),
        policy=PolicyEngine.from_file(settings.policy_path),
        model_factory=lambda agent: build_model(settings.llm_provider, settings.llm_model, agent),
        notifier=build_notifier(settings.notify_channel),
    )


def run_context(context: dict) -> RunContext:
    return RunContext(
        dag_id=context["dag"].dag_id,
        dag_run_id=context["run_id"],
        task_id=context["task"].task_id,
        base_url=Settings.from_env().airflow_base_url,
    )


class FixApprovalOperator(ApprovalOperator):
    """Airflow HITL approval that records the human decision in DagFox's approval table and audit log.

    The approval row binds the approver to the SHA-256 of the exact batch + diff
    they saw; the publish step re-verifies that hash before writing.
    """

    def __init__(self, *, proposal_task_id: str, **kwargs):
        # Assigned outright: Airflow pre-fills kwargs["params"] with the DAG's params,
        # which HITLOperator filters out of the approval form anyway.
        kwargs["params"] = {
            "comment": Param(None, type=["null", "string"], title="Comment (optional)", source="task",
                             description="Recorded in the DagFox audit log with your decision."),
        }
        super().__init__(**kwargs)
        self.proposal_task_id = proposal_task_id

    def _proposal(self, context) -> dict:
        return context["ti"].xcom_pull(task_ids=self.proposal_task_id, key="proposal")

    def execute_complete(self, context, event: dict[str, Any]) -> Any:
        # Record first: on Reject, ApprovalOperator skips downstream by raising, so nothing
        # after super().execute_complete() would run.
        pipeline, ctx, proposal = build_pipeline(), run_context(context), self._proposal(context)
        if "error" in event:
            if event.get("error_type") == "timeout":
                pipeline.record_human_decision(ctx, proposal, "timed_out", "system:timeout",
                                               "No response within the approval window")
            return super().execute_complete(context=context, event=event)  # raises

        self.validate_chosen_options(event["chosen_options"])
        chosen = event["chosen_options"][0]
        user = event.get("responded_by_user") or {}
        approver = user.get("name") or user.get("id") or "unknown"
        comment = (event.get("params_input") or {}).get("comment") or None
        approval_id = pipeline.record_human_decision(ctx, proposal, chosen, approver, comment)
        self.log.info("Recorded %s by %s as approval %s", chosen, approver, approval_id)
        context["ti"].xcom_push(key="approval", value={"chosen": chosen, "approver": approver,
                                                        "approval_id": approval_id})
        return super().execute_complete(context=context, event=event)
