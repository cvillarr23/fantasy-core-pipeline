from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest
from fastapi import HTTPException
from src.services import activity_runner as runner, activity_service as activity, league_dashboard, read_cache, llm_history
from src.api import routes


def test_draft_broker_failure_remains_retryable_with_durable_identity():
    payload = routes.DraftPickTriggerPayload(team_id='8',player_id='1',name='Player',position='RB',pro_team='KC',idempotency_key='pick')
    with patch.object(activity,'create_action',return_value='saved'),patch.object(runner,'publish_one',side_effect=RuntimeError('broker')):
        with pytest.raises(HTTPException) as error:
            routes._accept_durable('draft_targets',payload)
        assert error.value.status_code == 503


def test_duplicate_acceptance_is_explicit():
    payload=routes.WaiverTriggerPayload(team_id='8',player_id='1',name='Player',team='KC')
    with patch.object(activity,'create_action',return_value='original'),patch.object(runner,'publish_one'):
        assert routes._accept_durable('waiver_actions',payload)['status']=='duplicate'


def test_new_week_scopes_reused_key():
    c=MagicMock()
    c.fetchone.side_effect=[None,('new',),None,('newer',)]
    with patch.object(activity,'get_db_cursor') as db:
        db.return_value.__enter__.return_value=c
        for week in (5,6):
            activity.create_action('waiver_actions',{'team_id':'8','idempotency_key':'same','decision_inputs':{'season':2026,'week':week}})
    inserts=[call.args[1][-1] for call in c.execute.call_args_list if 'idempotency_key)' in call.args[0]]
    assert inserts == ['same:season:2026:week:5','same:season:2026:week:6']


@pytest.mark.parametrize('requested,actual',[('RB1','RB'),('WR2','WR'),('FLEX','RB/WR/TE')])
def test_numbered_and_flex_slots_verify(requested,actual):
    action={'league_id':'123','execution':'submitted','outcome':'unknown','kind':'lineup_updates','payload':{'season':2026,'team_id':'8','optimized_swaps':[{'add_player_id':'1','drop_player_id':'2','slot':requested}]}}
    league=SimpleNamespace(teams=[SimpleNamespace(team_id=8,roster=[SimpleNamespace(playerId=1,lineupSlot=actual),SimpleNamespace(playerId=2,lineupSlot='BE')])])
    with patch.object(runner,'action_detail',return_value=action),patch.object(runner,'record_event') as event,patch.object(runner,'get_db_cursor'),patch.object(read_cache,'invalidate_league'):
        runner.reconcile_action('id',{2026:league})
        assert event.call_args.kwargs['outcome']=='verified_complete'


def _reconcile_lineup(swaps, roster):
    action={'league_id':'123','execution':'submitted','outcome':'unknown','kind':'lineup_updates','payload':{'season':2026,'team_id':'8','optimized_swaps':swaps}}
    league=SimpleNamespace(teams=[SimpleNamespace(team_id=8,roster=[SimpleNamespace(playerId=pid,lineupSlot=slot) for pid,slot in roster])])
    with patch.object(runner,'action_detail',return_value=action),patch.object(runner,'record_event') as event,patch.object(runner,'get_db_cursor'),patch.object(read_cache,'invalidate_league'):
        runner.reconcile_action('id',{2026:league})
    return event.call_args


def test_ir_entries_verify_against_the_espn_roster():
    swaps=[
        {'add_player_id':'1','drop_player_id':'2','slot':'RB1'},
        {'kind':'ir','player_id':'3','slot':'IR'},
        {'kind':'activate_ir','player_id':'4','slot':'BENCH'},
    ]
    call=_reconcile_lineup(swaps,[(1,'RB'),(2,'BE'),(3,'IR'),(4,'BE')])
    assert call.kwargs['outcome']=='verified_complete'
    assert call.args[2]['items'][1:]==[
        {'player':'3','kind':'ir','confirmed':True},
        {'player':'4','kind':'activate_ir','confirmed':True},
    ]


def test_ir_entry_not_yet_applied_is_only_status_checked():
    call=_reconcile_lineup([{'kind':'ir','player_id':'3','slot':'IR'}],[(3,'BE')])
    assert call.args[1]=='status_checked'
    assert call.args[2]['items']==[{'player':'3','kind':'ir','confirmed':False}]


def test_tick_continues_past_poison_row_and_leases_reconciliation():
    c=MagicMock();c.fetchall.return_value=[]
    with patch.object(runner,'publish_one',side_effect=[RuntimeError('missing queue'),True,False]) as publish,patch.object(runner,'get_db_cursor') as db:
        db.return_value.__enter__.return_value=c
        runner.tick()
    assert publish.call_count==3
    assert 'FOR UPDATE SKIP LOCKED' in c.execute.call_args_list[0].args[0]
    assert 'historical' in c.execute.call_args_list[0].args[0]


def test_publish_failure_records_backoff():
    c=MagicMock();c.fetchone.return_value=('id','waiver_actions',{})
    with patch.object(runner,'get_db_cursor') as db,patch.object(runner.pika,'BlockingConnection',side_effect=RuntimeError('missing queue')):
        db.return_value.__enter__.return_value=c
        with pytest.raises(RuntimeError):runner.publish_one()
    assert any('next_attempt_at=now()' in call.args[0] and 'attempts=attempts+1' in call.args[0] for call in c.execute.call_args_list)


def test_publisher_preserves_trace_header_and_distinguishes_delivery():
    c=MagicMock();c.fetchone.return_value=('id','waiver_actions',{'traceparent':'trace'})
    connection=MagicMock()
    with patch.object(runner,'get_db_cursor') as db,patch.object(runner.pika,'BlockingConnection',return_value=connection):
        db.return_value.__enter__.return_value=c
        runner.publish_one()
    assert connection.channel.return_value.basic_publish.call_args.kwargs['properties'].headers == {'traceparent':'trace'}
    assert any("execution='delivered'" in call.args[0] for call in c.execute.call_args_list)


def test_unrelated_cache_keys_do_not_wait_for_slow_loader():
    entered=Event();release=Event()
    def slow():
        entered.set()
        assert release.wait(2)
        return 'slow'
    redis=MagicMock();redis.get.return_value=None
    with patch.object(read_cache,'get_redis_client',return_value=redis),ThreadPoolExecutor(max_workers=2) as pool:
        pending=pool.submit(read_cache.cached,['slow','league'],60,slow)
        assert entered.wait(1)
        try:
            assert pool.submit(read_cache.cached,['fast','league'],60,lambda:'fast').result(timeout=1)=='fast'
        finally:release.set()
        assert pending.result()=='slow'


def test_default_positions_use_player_id_space():
    assert [league_dashboard.DEFAULT_POSITIONS[k] for k in [1,2,3,4,5,16]]==['QB','RB','WR','TE','K','D/ST']


def test_history_wrapper_accepts_keyword_prompt_and_defaults_to_metadata(monkeypatch):
    monkeypatch.delenv('LLM_HISTORY_CAPTURE_CONTENT',raising=False)
    @llm_history.record_call
    def complete(prompt,system_prompt=None):
        assert prompt=='private league context'
        return SimpleNamespace(text='answer',provider='mock',model='mock',latency_ms=1)
    with patch.object(llm_history,'session_start',return_value='session'),patch.object(llm_history,'session_event') as event:
        complete(prompt='private league context',system_prompt='system')
    assert event.call_args_list[0].args[2]['capture']=='metadata only'
    assert 'private league context' not in str(event.call_args_list)
