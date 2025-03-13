import requests
import json
import os
from airflow.utils.email import send_email

def send_discord_alert(context):
    """
    Mengirim notifikasi ke Discord jika terjadi error pada DAG.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("[ERROR] DISCORD_WEBHOOK_URL tidak ditemukan dalam environment variables")
        return

    dag_id = context.get('dag_run').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    
    message = {
        "username": "Airflow Alert",
        "embeds": [
            {
                "title": f"DAG {dag_id} Failed!",
                "description": f"Task **{task_id}** gagal pada {execution_date}",
                "color": 16711680,  # Warna merah dalam format decimal
                "fields": [
                    {
                        "name": "Log",
                        "value": f"[Klik di sini untuk melihat log]({log_url})"
                    }
                ]
            }
        ]
    }
    
    try:
        response = requests.post(webhook_url, data=json.dumps(message), headers={"Content-Type": "application/json"})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Gagal mengirim alert ke Discord: {e}")
