NGINX_SERVER_CONF_TEMPLATE = """
server {{
    listen       {port} default_server;
    listen       [::]:{port} default_server;
    server_name  _;
    {access_log_off}

    location / {{
        include         uwsgi_params;
        
        # change this to the location of the uWSGI socket file (set in uwsgi.ini)
        uwsgi_pass                  unix:{sock_filepath};
        uwsgi_ignore_client_abort   on;
    }}
}}

"""
