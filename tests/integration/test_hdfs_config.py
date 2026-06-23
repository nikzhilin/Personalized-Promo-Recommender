from __future__ import annotations

import xml.etree.ElementTree as ElementTree
from pathlib import Path

ROOT = Path(__file__).parents[2]


def _properties(path: Path) -> dict[str, str]:
    root = ElementTree.parse(path).getroot()
    return {
        property_node.findtext("name", default=""): property_node.findtext(
            "value", default=""
        )
        for property_node in root.findall("property")
    }


def test_hdfs_configuration_matches_storage_contract() -> None:
    core = _properties(ROOT / "docker/hadoop/core-site.xml")
    hdfs = _properties(ROOT / "docker/hadoop/hdfs-site.xml")

    assert core["fs.defaultFS"] == "hdfs://namenode:9000"
    assert core["hadoop.security.authentication"] == "simple"
    assert hdfs["dfs.replication"] == "2"
    assert hdfs["dfs.blocksize"] == str(64 * 1024 * 1024)
    assert hdfs["dfs.namenode.safemode.min.datanodes"] == "1"
    assert hdfs["dfs.datanode.du.reserved"] == str(1024**3)


def test_compose_pins_images_and_defines_distinct_datanode_volumes() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "apache/hadoop:3.4.3@sha256:" in compose
    assert "spark:3.5.8-scala2.12-java17-python3-ubuntu@sha256:" in compose
    assert "hdfs-datanode-1:/hadoop/dfs/data" in compose
    assert "hdfs-datanode-2:/hadoop/dfs/data" in compose
