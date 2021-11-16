# Databricks notebook source
# MAGIC %pip install poetry

# COMMAND ----------

poetry_command = "poetry add marshmallow@3.10.0 --lock"

# COMMAND ----------

import os
import shutil
import tempfile
import json
from urllib import request, parse

def project_root_path():
  return "/".join(os.path.normpath(os.getcwd()).split(os.sep)[:5])

def api_post(url, headers = [], body = ""):
  # data = parse.urlencode(body).encode()
  req =  request.Request(url, data=body.encode())
  for key, value in headers.items():
    req.add_header(key, value)
  return request.urlopen(req)

def put_repos_file(path, file_name, body):
  dbx_host = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["extraContext"]["api_url"]
  dbx_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  path = "/".join(["", "api", "2.0", "workspace-files", *os.path.normpath(path).split(os.sep)[2:], ""])

  url = f"{dbx_host}{path}{file_name}?overwrite=true"

  headers = {
      "Authorization": f"Bearer {dbx_token}"
  }
  return api_post(url=url, headers=headers, body=body)


src = project_root_path()
dst = tempfile.mkdtemp()

shutil.copy(src + "/pyproject.toml", dst)
shutil.copy(src + "/poetry.lock", dst)

os.chdir(dst)
split_command = poetry_command.split(" ")
split_command[0] = "~/.poetry/bin/poetry"
adjusted_command = " ".join(split_command)
subprocess.run(adjusted_command, shell=True)

with open('pyproject.toml',mode='r') as f:
  put_repos_file(
    path=src,
    file_name="pyproject.toml",
    body=f.read()
  )

with open('poetry.lock',mode='r') as f:
  put_repos_file(
    path=src,
    file_name="poetry.lock",
    body=f.read()
  )

os.chdir(src)

# COMMAND ----------


