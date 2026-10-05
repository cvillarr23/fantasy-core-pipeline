from unittest.mock import MagicMock,patch
import pytest
from src.services.activity_runner import publish_one

@pytest.mark.parametrize('queue',['lineup_updates','waiver_actions','draft_targets'])
def test_outbox_confirms_delivery_without_redeclaring_queue(queue):
    cursor=MagicMock();cursor.fetchone.return_value=('id',queue,{'tracking_id':'id'})
    connection=MagicMock();channel=connection.channel.return_value
    with patch('src.services.activity_runner.get_db_cursor') as db,patch('src.services.activity_runner.pika.BlockingConnection',return_value=connection),patch('src.api.routes._pick_slot_already_filled',return_value=False):
        db.return_value.__enter__.return_value=cursor
        assert publish_one('id')
    channel.queue_declare.assert_called_once_with(queue=queue,passive=True)
    channel.confirm_delivery.assert_called_once()
    assert channel.basic_publish.call_args.kwargs['mandatory'] is True
    connection.close.assert_called_once()


def test_outbox_rechecks_draft_deadline_after_broker_connect():
    cursor=MagicMock();cursor.fetchone.return_value=('id','draft_targets',{'expires_at':1})
    connection=MagicMock()
    with patch('src.services.activity_runner.get_db_cursor') as db,patch('src.services.activity_runner.pika.BlockingConnection',return_value=connection):
        db.return_value.__enter__.return_value=cursor
        assert publish_one('id')
    connection.channel.return_value.basic_publish.assert_not_called()
    assert any("outcome='expired'" in call.args[0] for call in cursor.execute.call_args_list)
    connection.close.assert_called_once()


def test_outbox_closes_connection_on_publish_failure_and_does_not_mark_delivered():
    cursor=MagicMock();cursor.fetchone.return_value=('id','waiver_actions',{})
    connection=MagicMock();connection.channel.return_value.basic_publish.side_effect=RuntimeError('lost ack')
    with patch('src.services.activity_runner.get_db_cursor') as db,patch('src.services.activity_runner.pika.BlockingConnection',return_value=connection):
        db.return_value.__enter__.return_value=cursor
        with pytest.raises(RuntimeError): publish_one('id')
    assert not any('published_at=now()' in call.args[0] for call in cursor.execute.call_args_list)
    connection.close.assert_called_once()
