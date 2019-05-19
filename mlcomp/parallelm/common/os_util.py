import subprocess


def service_installed(service_name):
    try:
        subprocess.check_output('service {} status'.format(service_name), shell=True, stderr=subprocess.STDOUT)
        return True
    except:
        return False
