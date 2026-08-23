from pathlib import Path


CI_WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"


def test_build_push_job_removes_direct_fantasy_infra_writes():
    content = CI_WORKFLOW.read_text(encoding="utf-8")
    assert "INFRA_DEPLOY_KEY" not in content
    assert "git clone git@github.com:cvillarr23/fantasy-infra.git" not in content


def test_build_push_job_publishes_mutable_tag_for_digest_tracking():
    content = CI_WORKFLOW.read_text(encoding="utf-8")
    assert "type=raw,value=main" in content
    assert "type=raw,value=latest" in content
    assert "${{ steps.build.outputs.digest }}" in content
