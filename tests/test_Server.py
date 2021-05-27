import pathlib as p
from VERAStatus.Server import server_settings_dict2settings, ServerSettings


def test_server_settings_dict2settings():
    assert (server_settings_dict2settings(
        {"host": "192.168.1.1",
         "port": 22,
         "user": "username",
         "password": "pass_word",
         "schedule_path": "/home/username/schedule"}) ==
            ServerSettings("192.168.1.1", 22, "username", "pass_word",
                           p.PurePosixPath("/home/username/schedule")))
