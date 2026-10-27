#!/usr/bin/env python3
"""BDP v7 · Laboratorio en la nube: el MISMO portable v7 de Linux, sin interfaz gráfica.

Funciona en GitHub Codespaces, SageMaker Studio Lab, Saturn Cloud, Kaggle o cualquier Linux x86-64.
Solo usa la librería estándar (corre con el Python que traiga la plataforma, 3.8+).

    python bdp_start.py install          # baja y verifica el portable v7 (~1.7 GB, una sola vez)
    python bdp_start.py up               # curso UP:   HDFS + Jupyter + Elasticsearch
    python bdp_start.py unir             # curso UNIR: HDFS + Jupyter + Kafka
    python bdp_start.py base             # solo HDFS + Jupyter
    python bdp_start.py datos            # baja el recorte de un estado y lo carga en HDFS (por omisión 01)
    python bdp_start.py status           # qué está encendido
    python bdp_start.py check            # verifica que todo funcione (sirve como evidencia)
    python bdp_start.py stop             # apaga todo limpio

Opciones:
    --home DIR      dónde vive el lab (por omisión $BDP_HOME o ~/bdp-v7)
    --estado CVE    clave del estado para `datos` (01 = Aguascalientes)
    --no-jupyter    no lanza JupyterLab (cuando la plataforma ya trae el suyo)

Al encender, registra el kernel "BDP v7" (Spark, Sedona y el cliente de Elasticsearch listos) para
JupyterLab y para los cuadernos de VS Code.

Autor: Dr. Abel Coronado · BDP v7 "Convergente".
"""
import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
from pathlib import Path

MANIFEST = "https://huggingface.co/datasets/abxda/bdp-lab/resolve/main/v7/manifest-v7.txt"
DATA_BASE = "https://huggingface.co/datasets/abxda/bdp-lab/resolve/main/v7/datos/estados"
KEY = "linux-amd64-portable"
ADD_OPENS = ("--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED "
             "--add-opens=java.base/java.nio=ALL-UNNAMED")
SERVICES = {  # servicio -> puerto de salud
    "namenode": 9870, "datanode": 9864, "elasticsearch": 9200, "kafka": 9092, "jupyter": 8888,
}
PROFILES = {"base": [], "up": ["elasticsearch"], "unir": ["kafka"]}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def wait_port(port, secs):
    for _ in range(secs):
        if port_open(port):
            return True
        time.sleep(1)
    return False


def file_uri(p):
    return "file://" + Path(p).as_posix()


# ---------------------------------------------------------------------------------------------------
class Lab:
    def __init__(self, home):
        self.home = Path(home).expanduser().resolve()
        self.p = self.home / "portable"
        self.run = self.home / "run"
        self.logs = self.home / "logs"
        self.py = self.p / "python" / "bin" / "python3"

    # --- instalación --------------------------------------------------------------------------------
    def manifest(self):
        text = urllib.request.urlopen(MANIFEST, timeout=60).read().decode()
        entries = {}
        for line in text.splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                entries[k.strip()] = v.strip()
        return {f: entries[f"{KEY}.{f}"] for f in ("file", "sha256", "url", "version")}

    def installed_version(self):
        try:
            return json.loads((self.p / "bdp-stack.json").read_text())["bdp"]
        except (OSError, ValueError, KeyError):
            return None

    def install(self, keep=False):
        if platform.system() != "Linux" or platform.machine() not in ("x86_64", "AMD64"):
            sys.exit(f"El lab en la nube necesita Linux x86-64 (este equipo: {platform.system()} {platform.machine()}).")
        m = self.manifest()
        if self.installed_version() == m["version"]:
            log(f"BDP v7 {m['version']} ya está instalado en {self.p}")
            return
        free = shutil.disk_usage(self.home if self.home.exists() else Path.home()).free / 2**30
        if free < 6:
            sys.exit(f"Espacio insuficiente: {free:.1f} GB libres; se necesitan ~6 GB (descarga + extracción).")
        dl = self.home / "downloads" / m["file"]
        log(f"Laboratorio BDP v7 {m['version']}")
        self.download(m["url"], dl, m["sha256"])
        log("sha256 verificado; extrayendo")
        if self.p.exists():
            shutil.rmtree(self.p)
        self.p.mkdir(parents=True)
        with tarfile.open(dl) as t:
            if hasattr(tarfile, "data_filter"):
                t.extractall(self.p, filter="data")
            else:
                t.extractall(self.p)
        if not keep:
            dl.unlink()
        log(f"Instalado BDP v7 {m['version']} en {self.p}")

    def download(self, url, dest, sha):
        """Descarga con avance y verificación sha256 (reutiliza el archivo si ya coincide)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and self.sha256(dest) == sha:
            return dest
        log(f"Descargando {dest.name}")
        with urllib.request.urlopen(url, timeout=300) as r, open(dest, "wb") as f:
            total, done, mark = int(r.headers.get("Content-Length") or 0), 0, -1
            for chunk in iter(lambda: r.read(1 << 20), b""):
                f.write(chunk)
                done += len(chunk)
                if total > 50 * 2**20 and done * 10 // total != mark:
                    mark = done * 10 // total
                    log(f"  {mark * 10:3d}% de {total / 2**30:.2f} GB")
        if self.sha256(dest) != sha:
            dest.unlink()
            sys.exit(f"{dest.name} no coincide con su sha256; vuelve a intentar.")
        return dest

    def datos(self, cve):
        """Recorte de un estado -> HDFS, en las rutas que usan los cuadernos UP (no cambian)."""
        if not port_open(9870):
            sys.exit("HDFS no está encendido. Primero:  python3 bdp_start.py up")
        m = json.loads(urllib.request.urlopen(f"{DATA_BASE}/{cve}/manifest.json", timeout=60).read())
        env = dict(os.environ, **self.env())
        hdfs = str(self.p / "hadoop" / "bin" / "hdfs")
        for a in m["archivos"]:
            local = self.download(f"{DATA_BASE}/{cve}/{a['archivo']}", self.home / "datos" / cve / a["archivo"], a["sha256"])
            subprocess.run([hdfs, "dfs", "-mkdir", "-p", a["hdfs"].rsplit("/", 1)[0]], env=env, capture_output=True)
            r = subprocess.run([hdfs, "dfs", "-put", "-f", str(local), a["hdfs"]], env=env, capture_output=True, text=True)
            if r.returncode:
                sys.exit(f"No pude subir {a['archivo']} a HDFS:\n{r.stderr[-400:]}")
            log(f"{a['archivo']:28s} {a['filas']:>9,} filas  ->  hdfs://{a['hdfs']}")
        log(f"Datos del estado {cve} listos en HDFS. Revísalos con:  hdfs dfs -ls -R /data/raw")

    @staticmethod
    def sha256(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    # --- entorno (el mismo que el launcher da en v7: internal/services/common.go) -------------------
    def site_packages(self):
        return next((self.p / "python" / "lib").glob("python3*/site-packages"))

    def env(self):
        p = self.p
        e = {
            "JAVA_HOME": str(p / "common_jdk"), "HADOOP_HOME": str(p / "hadoop"),
            "HADOOP_CONF_DIR": str(p / "hadoop" / "etc" / "hadoop"), "HADOOP_OPTS": ADD_OPENS,
            "HADOOP_HEAPSIZE_MAX": "512m", "SPARK_HOME": str(self.site_packages() / "pyspark"),
            "PYSPARK_PYTHON": str(self.py), "PYSPARK_DRIVER_PYTHON": str(self.py),
            "KAFKA_LOG4J_OPTS": "-Dlog4j2.configurationFile=" + file_uri(p / "kafka_kraft" / "config" / "tools-log4j2.yaml"),
            "JUPYTER_CONFIG_DIR": str(p / "jupyter-config"), "JUPYTER_DATA_DIR": str(p / "data" / "jupyter" / "data"),
            "JUPYTER_RUNTIME_DIR": str(p / "data" / "jupyter" / "runtime"), "BDP_NOTEBOOKS": str(self.notebooks()),
            "BDP_HOME": str(self.home),
        }
        bins = [p / "python" / "bin", p / "hadoop" / "bin", Path(e["SPARK_HOME"]) / "bin",
                p / "kafka_kraft" / "bin", p / "common_jdk" / "bin"]
        e["PATH"] = os.pathsep.join(map(str, bins)) + os.pathsep + os.environ.get("PATH", "")
        return e

    def notebooks(self):
        # Los cuadernos viven junto a este script (repositorio del curso: se ven en el explorador de
        # VS Code). Ojo: CODESPACE_VSCODE_FOLDER no existe cuando Codespaces corre postStartCommand.
        beside = Path(__file__).resolve().parent / "notebooks"
        if beside.is_dir():
            return beside
        ws = os.environ.get("CODESPACE_VSCODE_FOLDER")
        return Path(ws) / "notebooks" if ws else self.p / "notebooks"

    def write_env_sh(self):
        """bdp_env.sh: las terminales reconocen hdfs, kafka-topics.sh, spark-submit y python sin exports."""
        lines = ["# BDP v7 · entorno del laboratorio (generado por bdp_start.py)"]
        lines += [f'export {k}="{v}"' for k, v in self.env().items() if k != "PATH"]
        lines.append(f'export PATH="{os.pathsep.join(self.env()["PATH"].split(os.pathsep)[:5])}:$PATH"')
        env_sh = self.home / "bdp_env.sh"
        env_sh.write_text("\n".join(lines) + "\n")
        bashrc = Path.home() / ".bashrc"
        hook = f"[ -f {env_sh} ] && . {env_sh}  # BDP v7"
        if hook not in (bashrc.read_text() if bashrc.exists() else ""):
            with open(bashrc, "a") as f:
                f.write("\n" + hook + "\n")
        return env_sh

    # --- procesos ------------------------------------------------------------------------------------
    def spawn(self, name, args, cwd, extra_env=None):
        self.run.mkdir(parents=True, exist_ok=True)
        self.logs.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, **self.env(), **(extra_env or {}))
        out = open(self.logs / f"{name}.log", "a")
        proc = subprocess.Popen(args, cwd=str(cwd), env=env, stdout=out, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL, start_new_session=True)  # sobrevive a esta terminal
        (self.run / f"{name}.pid").write_text(str(proc.pid))
        return proc

    def pid(self, name):
        try:
            pid = int((self.run / f"{name}.pid").read_text())
            os.kill(pid, 0)
            return pid
        except (OSError, ValueError):
            return None

    def write_hadoop_xml(self):
        uri = file_uri(self.p)
        conf = self.p / "hadoop" / "etc" / "hadoop"
        (conf / "core-site.xml").write_text(
            '<?xml version="1.0" encoding="UTF-8"?>\n<configuration>\n'
            '  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>\n'
            f'  <property><name>hadoop.tmp.dir</name><value>{uri}/data/hadoop_tmp</value></property>\n'
            '</configuration>\n')
        props = {  # igual que internal/setup/hadoop_xml.go (v7)
            "dfs.replication": "1", "dfs.namenode.name.dir": f"{uri}/data/hdfs/namenode",
            "dfs.datanode.data.dir": f"{uri}/data/hdfs/datanode", "dfs.permissions.enabled": "false",
            "dfs.namenode.rpc-bind-host": "127.0.0.1", "dfs.namenode.http-address": "127.0.0.1:9870",
            "dfs.datanode.address": "127.0.0.1:9866", "dfs.datanode.http.address": "127.0.0.1:9864",
            "dfs.datanode.ipc.address": "127.0.0.1:9867", "dfs.datanode.hostname": "localhost",
            "dfs.client.use.datanode.hostname": "true", "dfs.datanode.use.datanode.hostname": "true",
            "dfs.webhdfs.user.provider.user.pattern": "^[A-Za-z0-9_][A-Za-z0-9._-]*[$]?$",
        }
        body = "\n".join(f"  <property><name>{k}</name><value>{v}</value></property>" for k, v in props.items())
        (conf / "hdfs-site.xml").write_text(f'<?xml version="1.0" encoding="UTF-8"?>\n<configuration>\n{body}\n</configuration>\n')

    def start_hdfs(self):
        self.write_hadoop_xml()
        hdfs = str(self.p / "hadoop" / "bin" / "hdfs")
        if not (self.p / "data" / "hdfs" / "namenode" / "current" / "VERSION").exists():
            log("Formateando HDFS (primera vez)")
            subprocess.run([hdfs, "namenode", "-format", "-nonInteractive", "-force"], env=dict(os.environ, **self.env()),
                           cwd=str(self.p / "hadoop"), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        for name in ("namenode", "datanode"):
            if self.pid(name) or port_open(SERVICES[name]):
                log(f"{name} ya está encendido")
                continue
            self.spawn(name, [hdfs, name], self.p / "hadoop")
            log(f"{name}: " + ("listo" if wait_port(SERVICES[name], 120) else "NO respondió (ver logs)"))

    def start_elasticsearch(self):
        if self.pid("elasticsearch") or port_open(9200):
            log("elasticsearch ya está encendido")
            return
        es = self.p / "elasticsearch"
        self.spawn("elasticsearch", [str(es / "bin" / "elasticsearch")], es,
                   {"ES_JAVA_HOME": str(self.p / "common_jdk"), "ES_JAVA_OPTS": "-Xms512m -Xmx512m"})
        healthy = False
        for _ in range(180):
            try:
                h = json.loads(urllib.request.urlopen("http://127.0.0.1:9200/_cluster/health", timeout=2).read())
                if h["status"] in ("green", "yellow"):
                    healthy = True
                    break
            except (OSError, ValueError):
                pass
            time.sleep(1)
        if healthy:  # réplicas 0: índices en verde con un solo nodo
            req = urllib.request.Request("http://127.0.0.1:9200/_index_template/bdp-replicas-0", method="PUT",
                                         data=b'{"index_patterns":["*"],"priority":1,"template":{"settings":{"number_of_replicas":0}}}',
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10).read()
        log("elasticsearch: " + ("listo" if healthy else "NO respondió (ver logs/elasticsearch.log)"))

    def kafka_cmd(self, cls, *args):
        k = self.p / "kafka_kraft"
        java = str(self.p / "common_jdk" / "bin" / "java")
        log4j = "-Dlog4j2.configurationFile=" + file_uri(k / "config" / "log4j2.yaml")
        return [java, log4j, "-cp", str(k / "libs" / "*"), cls, *args]

    def start_kafka(self):
        if self.pid("kafka") or port_open(9092):
            log("kafka ya está encendido")
            return
        k = self.p / "kafka_kraft"
        cfg = str(k / "config" / "kraft" / "server.properties")
        env = dict(os.environ, **self.env())
        if not (k / "data" / "data_kraft" / "meta.properties").exists():
            uuid = subprocess.run(self.kafka_cmd("kafka.tools.StorageTool", "random-uuid"), env=env, cwd=str(k),
                                  capture_output=True, text=True).stdout.strip().splitlines()[-1]
            subprocess.run(self.kafka_cmd("kafka.tools.StorageTool", "format", "-t", uuid, "-c", cfg), env=env,
                           cwd=str(k), stdout=subprocess.DEVNULL, check=True)
        java = str(self.p / "common_jdk" / "bin" / "java")
        args = self.kafka_cmd("kafka.Kafka", cfg)
        self.spawn("kafka", [java, "-Xms256m", "-Xmx512m"] + args[1:], k)
        log("kafka: " + ("listo" if wait_port(9092, 120) else "NO respondió (ver logs/kafka.log)"))

    def start_jupyter(self):
        if self.pid("jupyter") or port_open(8888):
            log("jupyter ya está encendido")
            return
        nb = self.notebooks()
        nb.mkdir(parents=True, exist_ok=True)
        self.spawn("jupyter", [str(self.py), "-m", "jupyterlab", "--port=8888", "--no-browser", "--ip=127.0.0.1",
                               f"--ServerApp.root_dir={nb}"], nb)
        log("jupyter: " + ("listo" if wait_port(8888, 90) else "NO respondió (ver logs/jupyter.log)"))

    def register_kernel(self):
        """Kernel "BDP v7": en el Jupyter de la plataforma (VS Code, Studio Lab, Saturn...) y en el del lab."""
        env = {k: v for k, v in self.env().items() if not k.startswith("JUPYTER_")}
        spec = json.dumps({"argv": [str(self.py), "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                           "display_name": "BDP v7 (Spark 4.1 · Python 3.12)", "language": "python", "env": env}, indent=2)
        for base in (Path.home() / ".local" / "share" / "jupyter", Path(self.env()["JUPYTER_DATA_DIR"])):
            kdir = base / "kernels" / "bdp-v7"
            kdir.mkdir(parents=True, exist_ok=True)
            (kdir / "kernel.json").write_text(spec)
        log("kernel 'BDP v7 (Spark 4.1 · Python 3.12)' registrado")

    def stop(self):
        for name in ("jupyter", "kafka", "elasticsearch", "datanode", "namenode"):
            pid = self.pid(name)
            if not pid:
                continue
            log(f"deteniendo {name} (PID {pid})")
            try:
                os.killpg(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
            for _ in range(30):
                try:
                    os.kill(pid, 0)
                    time.sleep(1)
                except OSError:
                    break
            else:
                os.killpg(pid, signal.SIGKILL)
            (self.run / f"{name}.pid").unlink()

    def status(self):
        rows = []
        for name, port in SERVICES.items():
            rows.append((name, self.pid(name), port, port_open(port)))
        print(f"\nBDP v7 en {self.p}  (versión {self.installed_version() or 'no instalada'})")
        for name, pid, port, up in rows:
            print(f"  {name:14s} puerto {port:<5d} {'ENCENDIDO' if up else 'apagado':10s} {('PID ' + str(pid)) if pid else ''}")
        return rows

    def urls(self):
        cs, dom = os.environ.get("CODESPACE_NAME"), os.environ.get("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN")
        url = (lambda port, path="": f"https://{cs}-{port}.{dom}{path}") if cs and dom else (lambda port, path="": f"http://127.0.0.1:{port}{path}")
        print("\nAccesos:")
        if port_open(8888):
            print(f"  JupyterLab (sin token): {url(8888, '/lab')}")
        print(f"  HDFS (NameNode UI):     {url(9870)}")
        if port_open(9200):
            print(f"  Elasticsearch:          {url(9200)}")
        print(f"  Terminal: los comandos hdfs, kafka-topics.sh, spark-submit y python ya funcionan "
              f"(o: source {self.home / 'bdp_env.sh'})\n")

    # --- verificación ----------------------------------------------------------------------------------
    def check(self):
        env = dict(os.environ, **self.env())
        res = []

        def ok(name, cond, detail=""):
            res.append((name, bool(cond), detail))
            print(f"  {'OK   ' if cond else 'FALLA'} {name} {detail}", flush=True)

        print("\nVerificación del laboratorio BDP v7:")
        hdfs = str(self.p / "hadoop" / "bin" / "hdfs")
        sample = self.home / "check.csv"
        sample.write_text("id,ciudad,valor\n" + "\n".join(f"{i},AGS,{i * 1.5}" for i in range(500)))
        subprocess.run([hdfs, "dfs", "-mkdir", "-p", "/bdp_check"], env=env, capture_output=True)
        r = subprocess.run([hdfs, "dfs", "-put", "-f", str(sample), "/bdp_check/"], env=env, capture_output=True, text=True)
        ok("HDFS: hdfs dfs -put", r.returncode == 0)

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None
        try:
            urllib.request.build_opener(NoRedirect).open(urllib.request.Request(
                "http://localhost:9870/webhdfs/v1/bdp_check/w.txt?op=CREATE&user.name=17064&overwrite=true", method="PUT"), timeout=30)
            loc = ""
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location", "")
        ok("WebHDFS (usuario numérico) redirige a localhost", loc.startswith("http://localhost:9864/"))

        es_on, kafka_on = port_open(9200), port_open(9092)
        code = f"""
from pyspark.sql import SparkSession
s = (SparkSession.builder.master('local[2]').config('spark.driver.memory','1g')
     .config('spark.sql.extensions','org.apache.sedona.sql.SedonaSqlExtensions').getOrCreate())
n = s.read.csv('hdfs://localhost:9000/bdp_check/check.csv', header=True).count()
extra = ''
from sedona.spark import SedonaContext
k = SedonaContext.create(s).sql("SELECT ST_Intersects(ST_Point(-102.29,21.88), ST_Buffer(ST_Point(-102.29,21.88),0.01)) ok").first().ok
extra += f' sedona={{k}}'
if {kafka_on}:
    s.createDataFrame([(f'm{{i}}',) for i in range(10)], 'value string').write.format('kafka') \\
        .option('kafka.bootstrap.servers','localhost:9092').option('topic','bdp_check').save()
    m = s.read.format('kafka').option('kafka.bootstrap.servers','localhost:9092').option('subscribe','bdp_check') \\
        .option('startingOffsets','earliest').load().count()
    extra += f' kafka_mensajes={{m}}'
if {es_on}:
    from elasticsearch import Elasticsearch, helpers
    es = Elasticsearch('http://localhost:9200', request_timeout=60)
    es.indices.delete(index='bdp_check', ignore_unavailable=True)
    es.indices.create(index='bdp_check', mappings={{'properties': {{'location': {{'type': 'geo_point'}}}}}})
    helpers.bulk(es, ({{'_index': 'bdp_check', '_source': {{'n': i, 'location': {{'lat': 21.88, 'lon': -102.29}}}}}} for i in range(100)))
    es.indices.refresh(index='bdp_check')
    extra += f" es_docs={{es.count(index='bdp_check')['count']}}"
import pyspark
print(f'RESULTADO pyspark={{pyspark.__version__}} filas_hdfs={{n}}{{extra}}')
s.stop()
"""
        r = subprocess.run([str(self.py), "-c", code], env=env, capture_output=True, text=True, timeout=900)
        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULTADO")), "")
        ok("Spark 4.1 lee HDFS + Sedona", "filas_hdfs=500" in line and "sedona=True" in line, line.replace("RESULTADO ", ""))
        if kafka_on:
            ok("Kafka <-> Spark", "kafka_mensajes=10" in line)
        if es_on:
            ok("Elasticsearch: bulk de 100 docs con geo_point", "es_docs=100" in line)
        if not line:
            print("    error:", (r.stderr or "").strip().splitlines()[-3:])
        passed = sum(1 for _, c, _ in res if c)
        print(f"\nResumen: {passed}/{len(res)} OK · {time.strftime('%Y-%m-%d %H:%M')} · "
              f"host={platform.node()} · BDP {self.installed_version()}")
        return passed == len(res)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("accion", choices=["install", "up", "unir", "base", "datos", "status", "check", "stop"])
    ap.add_argument("--home", default=os.environ.get("BDP_HOME", "~/bdp-v7"))
    ap.add_argument("--estado", default="01")
    ap.add_argument("--no-jupyter", action="store_true")
    ap.add_argument("--keep-download", action="store_true")
    a = ap.parse_args()
    lab = Lab(a.home)
    if a.accion == "install":
        lab.install(keep=a.keep_download)
        lab.write_env_sh()
    elif a.accion in PROFILES:
        if not lab.installed_version():
            lab.install(keep=a.keep_download)
        env_sh = lab.write_env_sh()
        log(f"Perfil {a.accion}: base (HDFS + Jupyter)" + (f" + {', '.join(PROFILES[a.accion])}" if PROFILES[a.accion] else ""))
        # El otro servicio opcional se apaga: cada curso usa solo el suyo (menos RAM).
        for other in ("elasticsearch", "kafka"):
            if other not in PROFILES[a.accion] and lab.pid(other):
                pid = lab.pid(other)
                log(f"apagando {other} (no lo usa este curso)")
                os.killpg(pid, signal.SIGTERM)
                (lab.run / f"{other}.pid").unlink()
        lab.start_hdfs()
        if "elasticsearch" in PROFILES[a.accion]:
            lab.start_elasticsearch()
        if "kafka" in PROFILES[a.accion]:
            lab.start_kafka()
        lab.register_kernel()
        if not a.no_jupyter:
            lab.start_jupyter()
        lab.status()
        lab.urls()
        log(f"Listo. Para terminales nuevas el entorno se carga solo (~/.bashrc -> {env_sh}).")
    elif a.accion == "datos":
        lab.datos(a.estado)
    elif a.accion == "status":
        lab.status()
        lab.urls()
    elif a.accion == "check":
        sys.exit(0 if lab.check() else 1)
    elif a.accion == "stop":
        lab.stop()
        lab.status()


if __name__ == "__main__":
    main()
