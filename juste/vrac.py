import os
import base64
import urllib
import json
import requests
import time
import aiofiles
import asyncio
import aiohttp
import re
import xml.etree.ElementTree as ET

def jupyter_upload(
    token: str, localFilePath: str, remoteFilePath: str, jupyterUrl: str
):
    """
    Uploads file to JupyterLab through API REST

    :param token: The authorization token issued by JupyterHub for authentification. Found at "https://<jupyterhub-url>/hub/token".
    :param localFilePath: The file path to the local content to upload.
    :param remoteFilePath: The file path to the remote content to create. The destination directory must exist and be within the home folder.
    :param jupyterUrl: The url to the jupyter server. Typically "https://<jupyterhub-url>/user/<username>".

    :return: server response
    """
    remotePath = urllib.parse.quote(remoteFilePath, safe="")
    uploadURL = f"{jupyterUrl}/api/contents/{remotePath}"
    fileName = localFilePath[1 + localFilePath.rfind(os.sep) :]
    headers = {}
    headers["Authorization"] = f"token {token}"

    with open(localFilePath, "r") as localFile:
        data = localFile.read()
        b64data = base64.b64encode(bytes(data, "utf-8")).decode("ascii")
        body = json.dumps(
            {
                "content": b64data,
                "name": fileName,
                "path": remoteFilePath,
                "format": "base64",
                "type": "file",
            }
        )

    return requests.put(uploadURL, data=body, headers=headers, verify=True)


async def download_file(
    url: str,
    session: aiohttp.ClientSession,
    semaphore_https: asyncio.Semaphore,
    semaphore_files: asyncio.Semaphore,
    output_dir: str = "./",
    output_filename: str = None,
    chunk_size: int = 8192,
):
    filename = output_filename if output_filename is not None else url.split("/")[-1]
    output_path = os.path.join(output_dir, filename)

    if os.path.exists(output_path):
        return output_path, True, "File already exist"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    async with semaphore_https, semaphore_files:
        success = False
        comment = ""

        try:
            time_start = time.time()
            async with session.get(url) as response, aiofiles.open(
                output_path, "wb"
            ) as file:
                if response.status > 399:
                    response.raise_for_status()

                async for chunk in response.content.iter_chunked(chunk_size):
                    await file.write(chunk)

                success = True
                comment = f"Completed in {time.time() - time_start}s"
        except Exception as e:
            comment = str(e)
            if os.path.exists(output_path):
                os.remove(output_path)

        return output_path, success, comment


async def smart_download(
    files: tuple[tuple[str, str, str]],
    cap_https: int,
    cap_files: int,
):
    semaphore_https = asyncio.Semaphore(cap_https)
    semaphore_files = asyncio.Semaphore(cap_files)
    async with aiohttp.ClientSession() as session:
        tasks = (
            download_file(
                url,
                session,
                semaphore_https,
                semaphore_files,
                output_dir,
                output_filename,
            )
            for url, output_dir, output_filename in files
        )
        return await asyncio.gather(*tasks)


async def extract_tds_catalog(url: str, file_regex: str = ".*"):
    xml_namespace = "{http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0}"
    dataset_xpath = ".//" + xml_namespace + "dataset[" + xml_namespace + "dataSize]"
    file_server = re.sub("/thredds/catalog/.*", "/thredds/fileServer/", url)

    catalog = requests.get(url)
    catalog_xml = ET.fromstring(catalog.content)

    return (
        file_server + dataset.attrib.get("urlPath")
        for dataset in catalog_xml.findall(dataset_xpath)
        if re.match(file_regex, dataset.attrib.get("name"))
    )
