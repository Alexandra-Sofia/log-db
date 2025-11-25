import psycopg2

from django.shortcuts import render
from .parsers.parser_runner import run_parser
from ui.ingest.loader import insert_logs
from .util.logger import logger

from ui.state import PARSED_CACHE

def parse_and_upload(request):
    global PARSED_CACHE

    context = {
        "stats": None,
        "uploaded": False,
        "error": None,
    }

    if request.method == "POST" and "parse" in request.POST:
        try:
            parsed = run_parser("./input-logfiles")
            logger.info(f"parsed: {parsed.keys()}")
            PARSED_CACHE = parsed
            context["stats"] = {
                "ACCESS": len(parsed["ACCESS"]),
                "HDFS_DATAXCEIVER": len(parsed["HDFS_DATAXCEIVER"]),
                "HDFS_NAMESYSTEM": len(parsed["HDFS_NAMESYSTEM"]),
            }
        except Exception as e:
            context["error"] = f"Parsing failed: {e}"
            raise e

        return render(request, "ui/parse_upload_page.html", context)

    if request.method == "POST" and "upload" in request.POST:
        if PARSED_CACHE is None:
            context["error"] = "No parsed data found. Please parse first."
        else:
            try:
                conn = psycopg2.connect(
                    dbname="logdb",
                    user="admin",
                    password="admin123!",
                    host="postgres",
                    port=5432,
                )
                logger.info("DB connection was successful!")
                insert_logs(conn, PARSED_CACHE)
                conn.close()
                context["uploaded"] = True
            except Exception as e:
                context["error"] = f"Upload failed: {e}"
                raise e

        # Show stats again below the success message
        if PARSED_CACHE:
            context["stats"] = {
                "ACCESS": len(PARSED_CACHE["ACCESS"]),
                "HDFS_DATAXCEIVER": len(PARSED_CACHE["HDFS_DATAXCEIVER"]),
                "HDFS_NAMESYSTEM": len(PARSED_CACHE["HDFS_NAMESYSTEM"]),
            }

        return render(request, "ui/parse_upload_page.html", context)

    return render(request, "ui/parse_upload_page.html", context)
