
web: bash -lc 'mkdir -p /var/data && export DB_PATH=/var/data/school.db && gunicorn -w 2 -k gthread -t 120 -b 0.0.0.0:$PORT sec_app:app'
