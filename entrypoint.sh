pip install -q -e .
#[ -d "/funcx" ] && pip install -q -e /funcx
uwsgi --ini funcx_forwarder.ini