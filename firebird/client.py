from django.db.backends import BaseDatabaseClient
from django.conf import settings
import os

class DatabaseClient(BaseDatabaseClient):
    executable_name = 'isql'

    def runshell(self):
        args = [self.executable_name]
        settings_dict = self.connection.settings_dict
        if settings_dict['DATABASE_USER']:
            args += ["-u", settings_dict['DATABASE_USER']]
        if settings_dict['DATABASE_PASSWORD']:
            args += ["-p", settings_dict['DATABASE_PASSWORD']]
        if settings_dict['DATABASE_HOST']:
            args.append(settings_dict['DATABASE_HOST'] + ':' + settings_dict['DATABASE_NAME'])
        else:
            args.append(settings_dict['DATABASE_NAME'])
        os.system(' '.join(args))
