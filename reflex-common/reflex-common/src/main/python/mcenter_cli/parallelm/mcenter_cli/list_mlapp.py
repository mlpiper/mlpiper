from parallelm.mlapp_directory.mlapp_defs import MLAppProfileKeywords


def list_mlapp(mclient):
    fmt = "{:<35} {:<35}"
    s = ""
    s += fmt.format("Profile Name", "Pattern Name")
    for profile_info in mclient.list_ion_profiles():
        profile_name = profile_info[MLAppProfileKeywords.NAME]
        pattern_name = profile_info[MLAppProfileKeywords.PATTERN_NAME]
        if len(s) > 0:
            s += "\n"
        s += fmt.format(profile_name, pattern_name)
    print(s)
