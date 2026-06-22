import json
import os
import requests
import logging

logger = logging.getLogger("crashboard-dashboard")

def create_panel_base(id_num, title, p_type, x, y, w, h):
    return {
        "id": id_num,
        "title": title,
        "type": p_type,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "prometheus", "uid": "Prometheus"},
        "targets": []
    }

def generate_dashboard_dict(services, has_redis=True, has_kafka=True):
    panels = []
    
    # Panel 1: HTTP Latency (Line Chart)
    p_latency = create_panel_base(1, "HTTP Request Latency (ms)", "timeseries", 0, 0, 12, 8)
    p_latency.update({
        "fieldConfig": {
            "defaults": {
                "custom": {"drawStyle": "line", "lineInterpolation": "smooth"},
                "unit": "ms"
            }
        },
        "targets": [
            {
                "expr": 'sum(rate(http_request_duration_seconds_sum[15s])) by (job) / sum(rate(http_request_duration_seconds_count[15s])) by (job) * 1000',
                "legendFormat": "{{job}}",
                "refId": "A"
            }
        ]
    })
    panels.append(p_latency)

    # Panel 2: HTTP Error Rates (Line Chart)
    p_errors = create_panel_base(2, "HTTP Error Rates (%)", "timeseries", 12, 0, 12, 8)
    p_errors.update({
        "fieldConfig": {
            "defaults": {
                "custom": {"drawStyle": "line"},
                "unit": "percent"
            }
        },
        "targets": [
            {
                "expr": 'sum(rate(http_requests_total{status!~"2.."}[15s])) by (job) / sum(rate(http_requests_total[15s])) by (job) * 100',
                "legendFormat": "{{job}}",
                "refId": "A"
            }
        ]
    })
    panels.append(p_errors)

    # Panel 3: Container CPU Usage (Line Chart)
    p_cpu = create_panel_base(3, "Container CPU Usage (%)", "timeseries", 0, 8, 8, 8)
    p_cpu.update({
        "fieldConfig": {
            "defaults": {
                "custom": {"drawStyle": "line"},
                "unit": "percent"
            }
        },
        "targets": [
            {
                "expr": 'container_cpu_usage_percent',
                "legendFormat": "{{container_name}}",
                "refId": "A"
            }
        ]
    })
    panels.append(p_cpu)

    # Panel 4: Container Memory Usage (Line Chart)
    p_mem = create_panel_base(4, "Container Memory Usage (MB)", "timeseries", 8, 8, 8, 8)
    p_mem.update({
        "fieldConfig": {
            "defaults": {
                "custom": {"drawStyle": "line"},
                "unit": "megabytes"
            }
        },
        "targets": [
            {
                "expr": 'container_memory_usage_bytes / (1024 * 1024)',
                "legendFormat": "{{container_name}}",
                "refId": "A"
            }
        ]
    })
    panels.append(p_mem)

    # Optional Panels
    x_pos = 16
    
    if has_redis:
        p_redis = create_panel_base(5, "Redis Cache Hit Ratio", "gauge", x_pos, 8, 4, 8)
        p_redis.update({
            "fieldConfig": {
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                    "max": 100,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "orange", "value": 50},
                            {"color": "green", "value": 80}
                        ]
                    }
                }
            },
            "targets": [
                {
                    "expr": '(sum(rate(cache_hits_total[15s])) / (sum(rate(cache_hits_total[15s])) + sum(rate(cache_misses_total[15s])))) * 100',
                    "legendFormat": "Hit Ratio",
                    "refId": "A"
                }
            ]
        })
        panels.append(p_redis)
        x_pos += 4

    if has_kafka:
        p_kafka = create_panel_base(6, "Kafka Consumer Lag", "timeseries", x_pos, 8, 4, 8)
        p_kafka.update({
            "fieldConfig": {
                "defaults": {
                    "custom": {"drawStyle": "line"},
                    "unit": "none"
                }
            },
            "targets": [
                {
                    "expr": 'kafka_consumer_lag',
                    "legendFormat": "{{topic}} backlog",
                    "refId": "A"
                }
            ]
        })
        panels.append(p_kafka)

    # Panel 7: Container Statuses (Stat Panel at bottom)
    p_status = create_panel_base(7, "Container Statuses (Online/Offline)", "stat", 0, 16, 24, 4)
    p_status.update({
        "options": {
            "colorMode": "background",
            "graphMode": "none",
            "justifyMode": "center",
            "orientation": "horizontal",
            "textMode": "name"
        },
        "fieldConfig": {
            "defaults": {
                "mappings": [
                    {"type": "value", "options": {"1": {"color": "green", "text": "ONLINE"}}},
                    {"type": "value", "options": {"0": {"color": "red", "text": "OFFLINE"}}}
                ]
            }
        },
        "targets": [
            {
                "expr": 'container_status',
                "legendFormat": "{{container_name}}",
                "refId": "A"
            }
        ]
    })
    panels.append(p_status)

    dashboard = {
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                    "enable": True,
                    "hide": True,
                    "name": "Annotations & Alerts",
                    "type": "dashboard"
                }
            ]
        },
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": None,
        "links": [],
        "liveNow": False,
        "panels": panels,
        "refresh": "1s",
        "schemaVersion": 38,
        "style": "dark",
        "tags": ["chaos", "crashboard"],
        "templating": {"list": []},
        "time": {"from": "now-5m", "to": "now"},
        "timepicker": {},
        "timezone": "",
        "title": "CrashBoard Chaos Analytics",
        "uid": "crashboard-chaos",
        "version": 1,
        "weekStart": ""
    }
    
    return dashboard

def write_dashboard_json(services, has_redis=True, has_kafka=True, output_path=None):
    db_dict = generate_dashboard_dict(services, has_redis, has_kafka)
    
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(db_dict, f, indent=2)
        logger.info(f"Grafana dashboard JSON successfully written to: {output_path}")
        
    return db_dict
