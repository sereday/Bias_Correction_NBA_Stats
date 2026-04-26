import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src import config as cfg
from src.stages import import_stage


def main():
    job = cfg.load("job_request.json")
    active_stages = job.get("active_stages", job.get("stages", []))
    output_dir = job.get("output_dir", "./data/raw")

    for stage in active_stages:
        print(f"\n{'='*50}")
        print(f"Stage: {stage}")
        print("=" * 50)

        if stage == "import":
            import_stage.run(job, output_dir)
        elif stage == "clean":
            print("  (not yet implemented)")
        elif stage == "model":
            print("  (not yet implemented)")
        elif stage == "results":
            print("  (not yet implemented)")
        else:
            print(f"  Unknown stage: {stage}")


if __name__ == "__main__":
    main()
