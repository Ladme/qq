# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

from qq_lib.batch.pbs.pbs import PBS
from qq_lib.core.operator import QQOperator
from qq_lib.properties.states import RealState


def test_qqoperator_init_with_host(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    host = "example.host.org"

    informer_mock = MagicMock()
    informer_mock.batch_system = PBS
    informer_mock.getRealState.return_value = RealState.RUNNING

    with patch(
        "qq_lib.core.operator.QQInformer.fromFile", return_value=informer_mock
    ) as from_file_mock:
        operator = QQOperator(info_file, host)

    from_file_mock.assert_called_once_with(info_file, host)
    assert operator._informer == informer_mock
    assert operator._info_file == info_file
    assert operator._input_machine == host
    assert operator._batch_system is PBS
    assert operator._state == RealState.RUNNING


def test_qqoperator_init_without_host(tmp_path):
    info_file = tmp_path / "job.qqinfo"

    informer_mock = MagicMock()
    informer_mock.batch_system = PBS
    informer_mock.getRealState.return_value = RealState.RUNNING

    with patch(
        "qq_lib.core.operator.QQInformer.fromFile", return_value=informer_mock
    ) as from_file_mock:
        operator = QQOperator(info_file)

    from_file_mock.assert_called_once_with(info_file, None)
    assert operator._informer == informer_mock
    assert operator._info_file == info_file
    assert operator._input_machine is None
    assert operator._batch_system is PBS
    assert operator._state == RealState.RUNNING


def test_qqoperator_update(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    host = "example.host.org"

    old_informer = MagicMock()
    new_informer = MagicMock()
    new_informer.getRealState.return_value = RealState.RUNNING

    operator = QQOperator.__new__(QQOperator)
    operator._informer = old_informer
    operator._info_file = info_file
    operator._input_machine = host

    with patch(
        "qq_lib.core.operator.QQInformer.fromFile", return_value=new_informer
    ) as from_file_mock:
        operator.update()

    from_file_mock.assert_called_once_with(info_file, host)
    assert operator._informer == new_informer
    assert operator._state == RealState.RUNNING


def test_qqoperator_get_informer():
    informer_mock = MagicMock()
    operator = QQOperator.__new__(QQOperator)
    operator._informer = informer_mock

    result = operator.getInformer()

    assert result == informer_mock


def test_qqoperator_matches_job_returns_true():
    operator = QQOperator.__new__(QQOperator)
    operator._informer = MagicMock()
    operator._informer.matchesJob.return_value = True

    assert operator.matchesJob("12345") is True
    operator._informer.matchesJob.assert_called_once_with("12345")


def test_qqoperator_matches_job_returns_false():
    operator = QQOperator.__new__(QQOperator)
    operator._informer = MagicMock()
    operator._informer.matchesJob.return_value = False

    assert operator.matchesJob("99999") is False
    operator._informer.matchesJob.assert_called_once_with("99999")


def test_qqoperator_print_info():
    operator = QQOperator.__new__(QQOperator)
    operator._informer = MagicMock()
    mock_console = MagicMock()
    mock_panel = MagicMock()

    with patch("qq_lib.core.operator.QQPresenter") as mock_presenter_cls:
        mock_presenter_instance = MagicMock()
        mock_presenter_cls.return_value = mock_presenter_instance
        mock_presenter_instance.createJobStatusPanel.return_value = mock_panel

        operator.printInfo(mock_console)

        mock_presenter_cls.assert_called_once_with(operator._informer)
        mock_presenter_instance.createJobStatusPanel.assert_called_once_with(
            mock_console
        )
        mock_console.print.assert_called_once_with(mock_panel)
