"""Tests for Delta storage module."""

from unittest.mock import MagicMock

from braindrop.ingestion.delta_storage import create_arxiv_metadata_table


def test_create_arxiv_metadata_table_success() -> None:
    """Test that create_arxiv_metadata_table calls spark.sql correctly."""
    mock_spark = MagicMock()

    create_arxiv_metadata_table(mock_spark, "dev", "braindrop")

    # Verify spark.sql was called
    mock_spark.sql.assert_called_once()
    args, _ = mock_spark.sql.call_args
    sql_command = args[0]

    # Verify SQL command contains key elements
    assert "CREATE TABLE IF NOT EXISTS `dev`.`braindrop`.`arxiv_metadata`" in sql_command
    assert "USING DELTA" in sql_command
    assert "id STRING COMMENT 'Arxiv ID'" in sql_command
    assert "title STRING" in sql_command
