print(__file__)
print(__name__)
print(__package__)

if __name__ == "__main__":
    if not __package__:
        __package__ = "parallelm"
    print("[{}]".format(__package__))
    from parallelm.mcenter_cli.main import main
    main()
