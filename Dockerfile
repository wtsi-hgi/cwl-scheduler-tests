FROM python:3.6

COPY interval_list /interval_list

# Once mercury/split-interval-list is updated with the Python 3 fix,
# this can be used to get the file straight from there.
#COPY --from=mercury/split-interval-list /split_interval_list.py /split_interval_list.py
COPY split_interval_list.py /split_interval_list.py

COPY cwl-dummy-tool.py /cwl-dummy-tool.py

WORKDIR /
ENTRYPOINT ["python", "/cwl-dummy-tool.py"]
