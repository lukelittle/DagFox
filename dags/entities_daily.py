"""entities_daily: land the vendor's entity file, check it, triage problems, publish with sign-off.

    simulate_vendor_drop -> wait_for_file -> load_staging -> check_and_triage -> route
        route -> publish_clean                                   (all checks passed)
        route -> propose_fix -> approve_fix (HITL) -> publish_fix (SUPERVISED)
        route -> escalate                                        (HUMAN_LED: summary only)
        propose_fix -> escalate_after_review                     (diff failed validation/policy)

Late files are the AUTO band: the sensor waits and retries, logging each retry.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, PokeReturnValue, dag, get_current_context, task

from dagfox.airflow_support import FixApprovalOperator, build_pipeline, run_context
from dagfox.config import Settings
from dagfox.policy.engine import Policy
from dagfox.sources.landing import LandingZone
from dagfox.sources.vendor_sim import FAULTS

POLICY = Policy.load(Settings.from_env().policy_path)


def _batch_id(context) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])


def _file_name(context) -> str:
    return f"entities_{_batch_id(context)}.csv"


def _waited_minutes(context) -> float:
    started = context["dag_run"].start_date or datetime.now(UTC)
    return (datetime.now(UTC) - started).total_seconds() / 60


def _on_file_timeout(context) -> None:
    build_pipeline().late_file_timed_out(run_context(context), _waited_minutes(context))


@dag(
    dag_id="entities_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dagfox", "entities"],
    params={"fault": Param("none", enum=list(FAULTS), description="Demo fault to inject")},
    doc_md=__doc__,
)
def entities_daily():
    @task
    def simulate_vendor_drop() -> dict:
        """DEMO ONLY: write today's vendor file (with the requested fault) to the landing zone."""
        from dagfox.sources.vendor_sim import drop_daily_file

        context = get_current_context()
        settings = Settings.from_env()
        return drop_daily_file(
            LandingZone(settings.landing_dir),
            _file_name(context),
            LandingZone.read(settings.sample_path),
            context["params"]["fault"],
            settings.late_file_delay_seconds,
        )

    @task.sensor(
        poke_interval=15,
        timeout=timedelta(minutes=POLICY.late_file.max_wait_minutes),
        mode="reschedule",
        on_failure_callback=_on_file_timeout,
    )
    def wait_for_file() -> PokeReturnValue:
        """AUTO band: if the file is late, log the decision and retry on the next poke."""
        context = get_current_context()
        path = LandingZone(Settings.from_env().landing_dir).poll(_file_name(context))
        build_pipeline().late_file_poke(run_context(context), path is not None, _waited_minutes(context))
        return PokeReturnValue(is_done=path is not None, xcom_value=str(path) if path else None)

    @task
    def load_staging(path: str) -> str:
        batch_id = _batch_id(get_current_context())
        rows = build_pipeline().load_staging(batch_id, Path(path))
        print(f"Loaded {rows} rows into staging.entities as batch {batch_id}")
        return batch_id

    @task
    def check_and_triage(batch_id: str) -> dict:
        pipeline = build_pipeline()
        report = pipeline.check_batch(batch_id)
        print("\n".join(report.summary_lines()))
        decision = None
        if not report.passed:
            decision = pipeline.triage(run_context(get_current_context()), report)
            print(f"Band: {decision.band}. {decision.reason}")
        return {
            "report": report.model_dump(mode="json"),
            "decision": decision.model_dump(mode="json") if decision else None,
        }

    @task.branch
    def route(triaged: dict) -> str:
        if triaged["decision"] is None:
            return "propose_fix" if POLICY.publish.require_hitl_for_clean_loads else "publish_clean"
        return "propose_fix" if triaged["decision"]["band"] == "SUPERVISED" else "escalate"

    @task
    def publish_clean(triaged: dict) -> dict:
        from dagfox.checks.entity_checks import CheckReport

        pipeline, ctx = build_pipeline(), run_context(get_current_context())
        report = CheckReport.model_validate(triaged["report"])
        approval_id = pipeline.authorize_clean_load(ctx, report)
        return vars(pipeline.publish(ctx, report.batch_id, [], approval_id))

    @task.branch
    def propose_fix(triaged: dict) -> str:
        """SUPERVISED: explain, propose a dry-run diff, validate it, ask for approval."""
        from dagfox.checks.entity_checks import CheckReport
        from dagfox.policy.engine import PolicyDecision

        context = get_current_context()
        pipeline, ctx = build_pipeline(), run_context(context)
        report = CheckReport.model_validate(triaged["report"])
        if triaged["decision"] is None:
            proposal = pipeline.proposal_for_clean_load(ctx, report)
        else:
            proposal = pipeline.propose_fix(ctx, report, PolicyDecision.model_validate(triaged["decision"]))
        context["ti"].xcom_push(key="proposal", value=dict(proposal))
        if proposal["band"] != "SUPERVISED":
            print(f"Escalating: {proposal['decision']['reason']}")
            return "escalate"
        print(proposal["approval_body"])
        return "approve_fix"

    approve_fix = FixApprovalOperator(
        task_id="approve_fix",
        proposal_task_id="propose_fix",
        subject="DagFox: approve proposed fix for {{ run_id }}",
        body="{{ ti.xcom_pull(task_ids='propose_fix', key='proposal')['approval_body'] }}",
        assigned_users=[{"id": u, "name": u} for u in POLICY.approvals.approvers],
        response_timeout=timedelta(hours=POLICY.approvals.response_timeout_hours),
    )

    @task
    def publish_fix() -> dict:
        context = get_current_context()
        proposal = context["ti"].xcom_pull(task_ids="propose_fix", key="proposal")
        approval = context["ti"].xcom_pull(task_ids="approve_fix")
        pipeline = build_pipeline()
        result = pipeline.publish(run_context(context), proposal["batch_id"], proposal["changes"],
                                  approval["approval_id"])
        return vars(result)

    def _escalate(triaged: dict, after_review: bool) -> None:
        from dagfox.agents.models import Explanation
        from dagfox.checks.entity_checks import CheckReport
        from dagfox.policy.engine import PolicyDecision

        context = get_current_context()
        report = CheckReport.model_validate(triaged["report"])
        explanation = None
        if after_review:
            proposal = context["ti"].xcom_pull(task_ids="propose_fix", key="proposal")
            decision = PolicyDecision.model_validate(proposal["decision"])
            explanation = Explanation.model_validate(proposal["explanation"])
        else:
            decision = PolicyDecision.model_validate(triaged["decision"])
        result = build_pipeline().escalate(run_context(context), report, decision, explanation)
        print(result["explanation"]["summary"])

    @task
    def escalate(triaged: dict) -> None:
        """HUMAN_LED: gather context and write a summary. No proposed write."""
        _escalate(triaged, after_review=False)

    @task
    def escalate_after_review(triaged: dict) -> None:
        """HUMAN_LED: the proposed diff failed validation or policy review. No write."""
        _escalate(triaged, after_review=True)

    path = wait_for_file()
    simulate_vendor_drop() >> path
    triaged = check_and_triage(load_staging(path))
    fix = propose_fix(triaged)
    route(triaged) >> [publish_clean(triaged), fix, escalate(triaged)]
    fix >> [approve_fix, escalate_after_review(triaged)]
    approve_fix >> publish_fix()

entities_daily()
