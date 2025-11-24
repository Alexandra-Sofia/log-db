from django.shortcuts import render
from .util.logger import logger
from .parsers.parser_runner import run_parser

def home(request):
    return render(request, "ui/home.html")

def parse_logs(request):
    if request.method == "POST":
        logger.info("Parse button clicked.")

        parsed = run_parser("./input-logfiles")

        return render(
            request,
            "ui/parse_result.html",
            {
                "parsed": parsed,
                "counts": {
                    "access": len(parsed["access"]),
                    "dataxceiver": len(parsed["dataxceiver"]),
                    "namesystem": len(parsed["namesystem"]),
                }
            }
        )

    return HttpResponse("Invalid method", status=405)
