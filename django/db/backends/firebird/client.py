from django.conf import settings
import os

def runshell():
    args = [settings.DATABASE_NAME]
    args += ["-u %s" % settings.DATABASE_USER]
    if settings.DATABASE_PASSWORD:
        args += ["-p %s" % settings.DATABASE_PASSWORD]
    if 'FIREBIRD' not in os.environ:
        path = '/opt/firebird/bin/'
    os.system(path + 'isql ' + ' '.join(args))
