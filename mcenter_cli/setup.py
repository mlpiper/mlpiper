from setuptools import setup, find_packages


from parallelm.mcenter_cli import version  # This import should be placed after the copying of protobuf python sources

name = "mcenter-cli"
release = version
desc = "MCenter command line tool"
lic = "ParallelM"

required_python_pkgs = ["requests", "cliff"]

setup(
    name=name,
    namespace_packages=['parallelm'],
    version=release,
    description=desc,
    license=lic,
    zip_safe=False,
    scripts=["bin/mcenter-cli"],
    install_requires=required_python_pkgs,
    packages=find_packages(exclude=["build", "*.tests", "*.tests.*", "tests.*", "tests"]),
    py_modules=["__main__"],
    entry_points={
        'console_scripts': [
            'mcli = parallelm.mcenter_cli.mcli:main'
        ],
        'mcenter.cli': [
            'mlapp list= parallelm.mcenter_cli.command_mlapp:MLAppListCommand',
            'mlapp info= parallelm.mcenter_cli.command_mlapp:MLAppInfoCommand',
            'mlapp delete= parallelm.mcenter_cli.command_mlapp:MLAppDeleteCommand',
            'mlapp upload= parallelm.mcenter_cli.command_mlapp:MLAppUploadCommand',
            'mlapp download= parallelm.mcenter_cli.command_mlapp:MLAppDownloadCommand',

            'ee download= parallelm.mcenter_cli.command_ee:EEDownloadCommand',
            'ee upload= parallelm.mcenter_cli.command_ee:EEUploadCommand',
            'ee list= parallelm.mcenter_cli.command_ee:EEListCommand',

            'component upload= parallelm.mcenter_cli.command_component:ComponentUploadCommand',

            'model upload= parallelm.mcenter_cli.command_model:ModelUploadCommand',

            'action list= parallelm.mcenter_cli.command_action_log:ActionLogListCommand',

            'agent list= parallelm.mcenter_cli.command_agent:AgentListCommand',
            'agent register= parallelm.mcenter_cli.command_agent:AgentRegisterCommand',
        ],
    },

)

