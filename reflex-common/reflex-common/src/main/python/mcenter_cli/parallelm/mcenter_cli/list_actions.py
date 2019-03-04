import datetime

def list_actions(mclient, args):
    success = None
    if args.status == 'success':
        success = True
    elif args.status == 'failed':
        success = False

    object_list = None
    if args.object is not None:
        object_list = str(args.object).split(",")
    action_list = None
    if args.action is not None:
        action_list = str(args.action).split(",")
    user_list = None
    if args.username is not None:
        user_list = str(args.username).split(",")
    print("Getting action entries for: ")
    print("    Object: {}".format(object_list))
    print("    Action: {}".format(action_list))
    print("    Success: {}".format(success))
    print("    User: {}".format(user_list))

    log_entries = mclient.get_action_log(object_list, action_list, success, user_list)

    if log_entries is None:
        return

    fmt = "{:<35} {:<35} {:<10} {:<15} {:<6}"
    output = ""
    output += fmt.format("Time", "Object", "Action", "Username", "Success")
    for entry in log_entries:
        timestampStr = datetime.datetime.fromtimestamp(entry['timestampMillis']/1000).strftime('%Y-%m-%d %H:%M:%SZ')
        if len(output) > 0:
            output += '\n'
        output += fmt.format(timestampStr, entry['actionObject'], entry['action'], entry['username'], entry['success'])

    print(output)
