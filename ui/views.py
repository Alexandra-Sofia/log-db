from django.shortcuts import render
from .parsers.parser_runner import run_parser
from ui.ingest.loader import insert_logs
from ui.state import PARSED_CACHE
import psycopg

def parse_and_upload(request):
    global PARSED_CACHE

    context = {
        "stats": None,
        "uploaded": False,
        "error": None,
    }

    # ------------------------------------------------------
    # CASE 1: User clicks "Parse Logs"
    # ------------------------------------------------------
    if request.method == "POST" and "parse" in request.POST:
        try:
            parsed = run_parser("./input-logfiles")
            PARSED_CACHE = parsed
            context["stats"] = {
                "access": len(parsed["access"]),
                "dataxceiver": len(parsed["dataxceiver"]),
                "namesystem": len(parsed["namesystem"]),
            }
        except Exception as e:
            context["error"] = f"Parsing failed: {e}"

        return render(request, "ui/parse_upload_page.html", context)

    # ------------------------------------------------------
    # CASE 2: User clicks "Upload to DB"
    # ------------------------------------------------------
    if request.method == "POST" and "upload" in request.POST:
        if PARSED_CACHE is None:
            context["error"] = "No parsed data found. Please parse first."
        else:
            try:
                conn = psycopg.connect(
                    dbname="logdb",
                    user="admin",
                    password="admin123!",
                    host="postgres",
                    port=5432,
                )
                insert_logs(conn, PARSED_CACHE)
                conn.close()
                context["uploaded"] = True
            except Exception as e:
                context["error"] = f"Upload failed: {e}"

        # Show stats again below the success message
        if PARSED_CACHE:
            context["stats"] = {
                "access": len(PARSED_CACHE["access"]),
                "dataxceiver": len(PARSED_CACHE["dataxceiver"]),
                "namesystem": len(PARSED_CACHE["namesystem"]),
            }

        return render(request, "ui/parse_upload_page.html", context)

    # ------------------------------------------------------
    # CASE 3: GET request (page first load)
    # ------------------------------------------------------
    return render(request, "ui/parse_upload_page.html", context)
