# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import abstractmethod, ABCMeta
from configparser import ConfigParser
import os
from os.path import expanduser, join

from databricks_cli.utils import InvalidConfigurationError


_home = expanduser('~')
HOST = 'host'
USERNAME = 'username'
PASSWORD = 'password' # NOQA
TOKEN = 'token'
INSECURE = 'insecure'
DEFAULT_SECTION = 'DEFAULT'
_config_provider = None


def _get_path():
    return join(_home, '.databrickscfg')


def _fetch_from_fs():
    raw_config = ConfigParser()
    raw_config.read(_get_path())
    return raw_config


def _create_section_if_absent(raw_config, profile):
    if not raw_config.has_section(profile) and profile != DEFAULT_SECTION:
        raw_config.add_section(profile)


def _get_option_if_exists(raw_config, profile, option):
    if profile == DEFAULT_SECTION:
        # We must handle the DEFAULT_SECTION differently since it is not in the _sections property
        # of raw config.
        return raw_config.get(profile, option) if raw_config.has_option(profile, option) else None
    # Check if option is defined in the profile.
    elif option not in raw_config._sections.get(profile, {}).keys():
        return None
    return raw_config.get(profile, option)


def _set_option(raw_config, profile, option, value):
    if value:
        raw_config.set(profile, option, value)
    else:
        raw_config.remove_option(profile, option)


def _overwrite_config(raw_config):
    config_path = _get_path()
    with open(config_path, 'w') as cfg:
        raw_config.write(cfg)
    os.chmod(config_path, 0o600)


def update_and_persist_config(profile, databricks_config):
    """
    Takes a DatabricksConfig and adds the in memory contents to the persisted version of the
    config. This will overwrite any other config that was persisted to the file system under the
    same profile.
    :param databricks_config: DatabricksConfig
    """
    raw_config = _fetch_from_fs()
    _create_section_if_absent(raw_config, profile)
    _set_option(raw_config, profile, HOST, databricks_config.host)
    _set_option(raw_config, profile, USERNAME, databricks_config.username)
    _set_option(raw_config, profile, PASSWORD, databricks_config.password)
    _set_option(raw_config, profile, TOKEN, databricks_config.token)
    _set_option(raw_config, profile, INSECURE, databricks_config.insecure)
    _overwrite_config(raw_config)


def set_config_provider(provider):
    """
    Sets a ConfigProvider that will be used for all future calls to get_config(),
    used by the Databricks CLI code to discover the user's credentials.
    """
    global _config_provider
    if provider and not isinstance(provider, DatabrickConfigProvider):
        raise Exception('Must be instance of DatabrickConfigProvider: %s' % _config_provider)
    _config_provider = provider


def get_config_provider(provider):
    global _config_provider
    return _config_provider


def get_config():
    """
    Returns a DatabricksConfig containing the hostname and authentication used to talk to
    the Databricks API. By default, we leverage the DefaultConfigProvider to get
    this config, but this behavior may be overridden by calling 'set_config_provider'

    If no DatabricksConfig can be found, we will raise a NoDatabricksConfigException.
    """
    global _config_provider
    if _config_provider:
        config = _config_provider.get_config()
        if config:
            return config
        raise InvalidConfigurationError(
            'Custom provider returned no DatabricksConfig: %s' % _config_provider)

    config = DefaultConfigProvider().get_config()
    if config:
        return config
    raise InvalidConfigurationError.for_profile(None)


def get_config_for_profile(profile):
    """
    [Deprecated] Reads from the filesystem and gets a DatabricksConfig for the
    specified profile. If it does not exist, then return a DatabricksConfig with fields set
    to None.

    Internal callers should prefer get_config() to use user-specified overrides, and
    to return appropriate error messages as opposited to invalid configurations.

    If you want to read from a specific profile, please instead use
    'ProfileConfigProvider(profile).get_config()'.

    :return: DatabricksConfig
    """
    default_provider = DefaultConfigProvider(profile)
    config = default_provider.get_config()
    if config:
        return config
    return DatabricksConfig(None, None, None, None, None)


class DatabrickConfigProvider(object):
    __metaclass__ = ABCMeta
    @abstractmethod
    def get_config(self):
        pass


class DefaultConfigProvider(DatabrickConfigProvider):
    def __init__(self, profile=DEFAULT_SECTION):
        super(DatabrickConfigProvider, self).__init__()
        self.env_provider = EnvironmentVariableConfigProvider()
        self.default_profile_provider = ProfileConfigProvider(profile)

    def get_config(self):
        env_config = self.env_provider.get_config()
        if env_config:
            return env_config

        profile_config = self.default_profile_provider.get_config()
        if profile_config:
            return profile_config


class EnvironmentVariableConfigProvider(DatabrickConfigProvider):
    def get_config(self):
        host = os.environ.get('DATABRICKS_HOST')
        username = os.environ.get('DATABRICKS_USERNAME')
        password = os.environ.get('DATABRICKS_PASSWORD')
        token = os.environ.get('DATABRICKS_TOKEN')
        insecure = os.environ.get('DATABRICKS_INSECURE')
        config = DatabricksConfig(host, username, password, token, insecure)
        if config.is_valid:
            return config
        return None


class ProfileConfigProvider(DatabrickConfigProvider):
    def __init__(self, profile=DEFAULT_SECTION):
        super(DatabrickConfigProvider, self).__init__()
        self.profile = profile

    def get_config(self):
        if not os.path.exists(_get_path()):
            return None

        raw_config = _fetch_from_fs()
        host = _get_option_if_exists(raw_config, self.profile, HOST)
        username = _get_option_if_exists(raw_config, self.profile, USERNAME)
        password = _get_option_if_exists(raw_config, self.profile, PASSWORD)
        token = _get_option_if_exists(raw_config, self.profile, TOKEN)
        insecure = _get_option_if_exists(raw_config, self.profile, INSECURE)
        config = DatabricksConfig(host, username, password, token, insecure)
        if config.is_valid:
            return config
        return None


class DatabricksConfig(object):
    def __init__(self, host, username, password, token, insecure): # noqa
        self.host = host
        self.username = username
        self.password = password
        self.token = token
        self.insecure = insecure

    @classmethod
    def from_token(cls, host, token, insecure=None):
        return DatabricksConfig(host, None, None, token, insecure)

    @classmethod
    def from_password(cls, host, username, password, insecure=None):
        return DatabricksConfig(host, username, password, None, insecure)

    @property
    def is_valid_with_token(self):
        return self.host is not None and self.token is not None

    @property
    def is_valid_with_password(self):
        return self.host is not None and self.username is not None and self.password is not None

    @property
    def is_valid(self):
        return self.is_valid_with_token or self.is_valid_with_password
