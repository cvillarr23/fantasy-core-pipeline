from src import k8s_client


def test_env_override_wins(monkeypatch, tmp_path):
    namespace_file = tmp_path / "namespace"
    namespace_file.write_text("from-file\n")
    monkeypatch.setattr(k8s_client, "SERVICE_ACCOUNT_NAMESPACE_FILE", str(namespace_file))
    monkeypatch.setenv("K8S_NAMESPACE", "from-env")
    assert k8s_client._default_namespace() == "from-env"


def test_pod_namespace_file_used_without_env(monkeypatch, tmp_path):
    namespace_file = tmp_path / "namespace"
    namespace_file.write_text("fantasy\n")
    monkeypatch.setattr(k8s_client, "SERVICE_ACCOUNT_NAMESPACE_FILE", str(namespace_file))
    monkeypatch.delenv("K8S_NAMESPACE", raising=False)
    assert k8s_client._default_namespace() == "fantasy"


def test_falls_back_to_default(monkeypatch, tmp_path):
    monkeypatch.setattr(k8s_client, "SERVICE_ACCOUNT_NAMESPACE_FILE", str(tmp_path / "missing"))
    monkeypatch.delenv("K8S_NAMESPACE", raising=False)
    assert k8s_client._default_namespace() == "default"
