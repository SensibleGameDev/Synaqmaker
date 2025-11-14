import sys
import os
import sqlite3
import subprocess
import tempfile
import json
import platform
import shutil 
import time
# НАСТРОЙКИ БЕЗОПАСНОСТИ DOCKER
DOCKER_IMAGE_PYTHON = "testirovschik-python"
DOCKER_IMAGE_CPP = "testirovschik-cpp"

DOCKER_COMMON_ARGS = [
    "docker", "run",
    "--rm",             
    # "-i" больше не нужен
    "--network=none",   
    "--memory=256m",    
    "--cpus=1.0",       
    "--pids-limit=10",  
    "--user=appuser",   
    "-w", "/home/appuser/run" 
]

# --- Скрипт "внутреннего судьи" для Python (без изменений) ---
JUDGE_SCRIPT_PYTHON = """
import sys
import os
import json
import subprocess
import time

def run_judge():
    results = []
    
    try:
        with open('tests.json', 'r') as f:
            tests = json.load(f)
    except Exception as e:
        print(json.dumps([{"verdict": "Internal Error", "error": f"Failed to read tests.json: {e}"}]))
        return

    for i, test in enumerate(tests):
        test_input = test.get('input', '')
        expected_output = test.get('output', '')
        time_limit = float(test.get('limit', 1.0))
        safe_time_limit = max(1.0, time_limit) 
        
        try:
            start_time = time.monotonic()
            
            process = subprocess.run(
                ['timeout', str(safe_time_limit), 'python3', 'script.py'],
                input=test_input.encode('utf-8'),
                capture_output=True,
                timeout=safe_time_limit + 2.0 
            )
            
            duration = time.monotonic() - start_time
            output = process.stdout.decode('utf-8', errors='replace')
            error = process.stderr.decode('utf-8', errors='replace')
            return_code = process.returncode
            
            verdict = ""
            
            if return_code == 124:
                verdict = "Time Limit Exceeded"
            elif return_code != 0:
                verdict = "Runtime Error"
            else:
                norm_out = output.replace('\\r\\n', '\\n').strip()
                norm_exp = expected_output.replace('\\r\\n', '\\n').strip()
                
                if norm_out.split('\\n') == norm_exp.split('\\n'):
                    verdict = "Accepted"
                else:
                    verdict = "Wrong Answer"
            
            results.append({
                "test_num": i + 1,
                "verdict": verdict,
                "output": output,
                "error": error
            })

        except subprocess.TimeoutExpired:
            results.append({
                "test_num": i + 1,
                "verdict": "Time Limit Exceeded",
                "output": "",
                "error": "Judge subprocess timeout"
            })
        except Exception as e:
            results.append({
                "test_num": i + 1,
                "verdict": "Internal Error",
                "output": "",
                "error": str(e)
            })

    print(json.dumps(results))

if __name__ == "__main__":
    run_judge()
"""

# --- Скрипт "внутреннего судьи" для C++ (ИСПРАВЛЕН) ---
JUDGE_SCRIPT_CPP = """
import sys
import os
import json
import subprocess
import time

def run_judge():
    results = []
    
    # 1. Компиляция (ОДИН РАЗ)
    try:
        # --- ИСПРАВЛЕНИЕ 1: Компилируем в /tmp/a.out ---
        compile_proc = subprocess.run(
            ['g++', 'source.cpp', '-o', '/tmp/a.out', '-O2', '-std=c++17'],
            capture_output=True, text=True, timeout=10
        )
    except subprocess.TimeoutExpired:
        print(json.dumps([{"verdict": "Compilation Error", "error": "Compilation timed out (> 10s)"}]))
        return

    if compile_proc.returncode != 0:
        error_msg = compile_proc.stderr.replace("source.cpp:", "line ")
        print(json.dumps([{"verdict": "Compilation Error", "error": error_msg}]))
        return

    # 2. Загружаем список тестов
    try:
        with open('tests.json', 'r') as f:
            tests = json.load(f)
    except Exception as e:
        print(json.dumps([{"verdict": "Internal Error", "error": f"Failed to read tests.json: {e}"}]))
        return

    # 3. Прогоняем каждый тест
    for i, test in enumerate(tests):
        test_input = test.get('input', '')
        expected_output = test.get('output', '')
        time_limit = float(test.get('limit', 1.0))
        safe_time_limit = max(1.0, time_limit)
        
        try:
            start_time = time.monotonic()
            
            # --- ИСПРАВЛЕНИЕ 2: Запускаем /tmp/a.out ---
            process = subprocess.run(
                ['timeout', str(safe_time_limit), '/tmp/a.out'],
                input=test_input.encode('utf-8'),
                capture_output=True,
                timeout=safe_time_limit + 2.0
            )
            
            duration = time.monotonic() - start_time
            output = process.stdout.decode('utf-8', errors='replace')
            error = process.stderr.decode('utf-8', errors='replace')
            return_code = process.returncode
            
            verdict = ""
            
            if return_code == 124:
                verdict = "Time Limit Exceeded"
            elif return_code != 0:
                verdict = "Runtime Error"
            else:
                norm_out = output.replace('\\r\\n', '\\n').strip()
                norm_exp = expected_output.replace('\\r\\n', '\\n').strip()
                
                if norm_out.split('\\n') == norm_exp.split('\\n'):
                    verdict = "Accepted"
                else:
                    verdict = "Wrong Answer"
            
            results.append({
                "test_num": i + 1,
                "verdict": verdict,
                "output": output,
                "error": error
            })

        except subprocess.TimeoutExpired:
            results.append({
                "test_num": i + 1,
                "verdict": "Time Limit Exceeded",
                "output": "",
                "error": "Judge subprocess timeout"
            })
        except Exception as e:
            results.append({
                "test_num": i + 1,
                "verdict": "Internal Error",
                "output": "",
                "error": str(e)
            })

    # 4. Возвращаем JSON-массив со всеми результатами
    print(json.dumps(results))

if __name__ == "__main__":
    run_judge()
"""

def _get_docker_path(abs_path):
    r"""Конвертирует путь Windows (C:\...) в /c/... для Docker."""
    if platform.system() == "Windows":
         abs_path = abs_path.replace("\\", "/")
         if abs_path[1] == ":":
            abs_path = "/" + abs_path[0].lower() + abs_path[2:]
    return abs_path

class DBManager:
   
    def __init__(self, db_name="testirovschik.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        self._create_olympiad_tables()

    def _create_olympiad_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS olympiad_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        olympiad_id TEXT NOT NULL,
                        participant_uuid TEXT NOT NULL,
                        nickname TEXT NOT NULL,
                        total_score INTEGER,
                        task_scores TEXT, 
                        UNIQUE(olympiad_id, participant_uuid)
                    )''')

        c.execute("PRAGMA table_info(olympiad_results)")
        columns = [col[1] for col in c.fetchall()]
        if "organization" not in columns:
            print("INFO: Updating database. Adding 'organization' column to 'olympiad_results' table.")
            c.execute("ALTER TABLE olympiad_results ADD COLUMN organization TEXT")
            
        c.execute("PRAGMA table_info(olympiad_results)")
        columns = [col[1] for col in c.fetchall()]
        if "disqualified" not in columns:
            print("INFO: Updating database. Adding 'disqualified' column to 'olympiad_results' table.")
            c.execute("ALTER TABLE olympiad_results ADD COLUMN disqualified BOOLEAN DEFAULT 0")

        c.execute('''CREATE TABLE IF NOT EXISTS olympiad_submissions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        olympiad_id TEXT NOT NULL,
                        participant_uuid TEXT NOT NULL,
                        nickname TEXT NOT NULL,
                        task_submissions TEXT, 
                        UNIQUE(olympiad_id, participant_uuid)
                    )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS olympiad_whitelist (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        olympiad_id TEXT NOT NULL,
                        nickname TEXT NOT NULL,
                        organization TEXT,
                        password TEXT NOT NULL,
                        UNIQUE(olympiad_id, nickname) 
                    )''')
        self.conn.commit()

    def save_olympiad_data(self, olympiad_id, olympiad_data):
        c = self.conn.cursor()
        participants = olympiad_data.get('participants', {})
        for p_uuid, p_data in participants.items():
            nickname = p_data.get('nickname')
            organization = p_data.get('organization', None) 
            disqualified = p_data.get('disqualified', False)
            scores = p_data.get('scores', {})
            
            total_score = 0
            
            total_score = sum(s.get('score', 0) for s in scores.values())

            task_scores_json = json.dumps(scores) 
            # -------------------------------
            
            c.execute("""
                INSERT INTO olympiad_results (olympiad_id, participant_uuid, nickname, organization, total_score, task_scores, disqualified)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(olympiad_id, participant_uuid) DO UPDATE SET
                organization=excluded.organization, total_score=excluded.total_score, task_scores=excluded.task_scores,
                disqualified=excluded.disqualified
            """, (olympiad_id, p_uuid, nickname, organization, total_score, task_scores_json, disqualified))
            submissions = p_data.get('last_submissions', {})
            submissions_json = json.dumps(submissions)
            c.execute("""
                INSERT INTO olympiad_submissions (olympiad_id, participant_uuid, nickname, task_submissions)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(olympiad_id, participant_uuid) DO UPDATE SET
                task_submissions=excluded.task_submissions
            """, (olympiad_id, p_uuid, nickname, submissions_json))
        self.conn.commit()
        
    def get_olympiad_results(self, olympiad_id):
        c = self.conn.cursor()
        c.execute("""
            SELECT participant_uuid, nickname, organization, total_score, task_scores, disqualified
            FROM olympiad_results WHERE olympiad_id = ?
        """, (olympiad_id,)) # Убрали ORDER BY здесь, отсортируем в Python
        
        participants_raw = c.fetchall()
        if not participants_raw:
            return None 

        participants_list = []
        task_ids = set()
        
        for p in participants_raw:
            # BUGFIX #3: Читаем полный JSON
            scores_full = json.loads(p['task_scores']) 
            
            # Собираем ID задач для заголовка таблицы
            task_ids.update(scores_full.keys())
            
            # Пересчитываем итоговые штрафы/баллы, так как в базе в total_score лежит только сумма очков
            total_score_calc = 0
            total_penalty_calc = 0
            
            # Проходимся по задачам и считаем итоги
            for tid, info in scores_full.items():
                # Если старый формат базы (просто число), обрабатываем (на всякий случай)
                if isinstance(info, int):
                    info = {'score': info, 'attempts': 0, 'passed': False, 'penalty': 0}
                    scores_full[tid] = info # Обновляем до словаря
                
                total_score_calc += info.get('score', 0)
                if info.get('passed'):
                    total_penalty_calc += info.get('penalty', 0)

            participants_list.append({
                'nickname': p['nickname'],
                'organization': p['organization'],
                'scores': scores_full, # Теперь тут есть и штрафы, и попытки
                'total_score': total_score_calc,
                'total_penalty': total_penalty_calc,
                'disqualified': p['disqualified'] 
            })
            
        # Получаем детали задач
        task_ids_list = sorted(list(task_ids), key=int)
        tasks_details = [self.get_task_details(tid) for tid in task_ids_list]
        
        # Сортировка (ICPC или по очкам) - делаем "умную" сортировку
        # Сначала по очкам (убывание), потом по штрафам (возрастание)
        participants_list.sort(key=lambda p: (-p['total_score'], p['total_penalty']))

        return {
            'results': {
                'status': 'finished', 
                'config': {'olympiad_id': olympiad_id, 'scoring': 'icpc'} # Можно попытаться сохранить scoring в БД отдельно, но пока так
            }, 
            'tasks': tasks_details,
            'participants_list': participants_list
        }

    def create_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS tasks (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         title TEXT, difficulty TEXT, topic TEXT, description TEXT
                       )''')
        columns = [col[1] for col in c.execute("PRAGMA table_info(tasks)").fetchall()]
        if "attachment" not in columns:
            c.execute("ALTER TABLE tasks ADD COLUMN attachment BLOB")
        if "file_format" not in columns:
            c.execute("ALTER TABLE tasks ADD COLUMN file_format TEXT")
        c.execute('''CREATE TABLE IF NOT EXISTS tests (
                         id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER,
                         test_input TEXT, expected_output TEXT, time_limit REAL,
                         FOREIGN KEY(task_id) REFERENCES tasks(id)
                       )''')
        c.execute('''CREATE TABLE IF NOT EXISTS submissions (
                         id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER,
                         language TEXT, code TEXT, result TEXT,
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                         FOREIGN KEY(task_id) REFERENCES tasks(id)
                       )''')
        self.conn.commit()
 

    def get_whitelist_for_olympiad(self, olympiad_id):
        c = self.conn.cursor()
        c.execute("""
            SELECT id, nickname, organization 
            FROM olympiad_whitelist 
            WHERE olympiad_id = ? 
            ORDER BY nickname
        """, (olympiad_id,))
        return c.fetchall()

    def add_participant_to_whitelist(self, olympiad_id, nickname, organization, password):
        c = self.conn.cursor()
        try:
            c.execute("""
                INSERT INTO olympiad_whitelist (olympiad_id, nickname, organization, password)
                VALUES (?, ?, ?, ?)
            """, (olympiad_id, nickname, organization, password))
            self.conn.commit()
            return (True, f"Участник {nickname} добавлен.")
        except sqlite3.IntegrityError:
            return (False, f"Ошибка: Участник {nickname} уже в списке этой олимпиады.")
        except Exception as e:
            return (False, f"Ошибка базы данных: {e}")

    def remove_participant_from_whitelist(self, participant_db_id):
        c = self.conn.cursor()
        try:
            c.execute("DELETE FROM olympiad_whitelist WHERE id = ?", (participant_db_id,))
            self.conn.commit()
            return c.rowcount > 0 
        except Exception as e:
            print(f"Ошибка при удалении участника: {e}")
            return False

    def validate_closed_participant(self, olympiad_id, nickname, password):
        c = self.conn.cursor()
        c.execute("""
            SELECT * FROM olympiad_whitelist 
            WHERE olympiad_id = ? AND nickname = ? AND password = ?
        """, (olympiad_id, nickname, password))
        return c.fetchone()
        
    def add_task(self, title, difficulty, topic, description, attachment, file_format):
        c = self.conn.cursor()
        c.execute("INSERT INTO tasks (title, difficulty, topic, description, attachment, file_format) VALUES (?,?,?,?,?,?)",
                  (title, difficulty, topic, description, attachment, file_format))
        self.conn.commit()

    def get_tasks(self):
        c = self.conn.cursor()
        c.execute("SELECT id, title, difficulty, topic FROM tasks ORDER BY id DESC")
        return c.fetchall()

    def get_task_details(self, task_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
        return c.fetchone()

    def update_task(self, task_id, title, difficulty, topic, description, attachment, file_format):
        c = self.conn.cursor()
        if attachment and file_format:
             c.execute("""UPDATE tasks SET title=?, difficulty=?, topic=?, description=?, attachment=?, file_format=? 
                         WHERE id=?""",
                       (title, difficulty, topic, description, attachment, file_format, task_id))
        else:
            c.execute("""UPDATE tasks SET title=?, difficulty=?, topic=?, description=?
                         WHERE id=?""",
                       (title, difficulty, topic, description, task_id))
        self.conn.commit()


    def delete_task(self, task_id):
        c = self.conn.cursor()
        c.execute("DELETE FROM tests WHERE task_id=?", (task_id,))
        c.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        self.conn.commit()

    def add_test(self, task_id, test_input, expected_output, time_limit):
        c = self.conn.cursor()
        c.execute("INSERT INTO tests (task_id, test_input, expected_output, time_limit) VALUES (?,?,?,?)",
                  (task_id, test_input, expected_output, time_limit))
        self.conn.commit()

    def get_tests_for_task(self, task_id):
        c = self.conn.cursor()
        c.execute("SELECT id, test_input, expected_output, time_limit FROM tests WHERE task_id=?", (task_id,))
        return c.fetchall()

    def get_test_details(self, test_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM tests WHERE id=?", (test_id,))
        return c.fetchone()

    def update_test(self, test_id, test_input, expected_output, time_limit):
        c = self.conn.cursor()
        c.execute("""UPDATE tests SET test_input=?, expected_output=?, time_limit=? 
                       WHERE id=?""", (test_input, expected_output, time_limit, test_id))
        self.conn.commit()

    def delete_test(self, test_id):
        c = self.conn.cursor()
        c.execute("DELETE FROM tests WHERE id=?", (test_id,))
        self.conn.commit()
    
    def add_submission(self, task_id, language, code, result):
        c = self.conn.cursor()
        c.execute("INSERT INTO submissions (task_id, language, code, result) VALUES (?,?,?,?)",
                  (task_id, language, code, result))
        self.conn.commit()
    # --- ДОБАВИТЬ ЭТОТ МЕТОД В КЛАСС DBManager ---
    def get_participant_progress(self, olympiad_id, participant_uuid):
        """Восстанавливает прогресс участника из базы данных, если он есть."""
        c = self.conn.cursor()
        
        # 1. Достаем баллы и статус
        c.execute("""
            SELECT task_scores, disqualified, organization 
            FROM olympiad_results 
            WHERE olympiad_id = ? AND participant_uuid = ?
        """, (olympiad_id, participant_uuid))
        row_res = c.fetchone()
        
        if not row_res:
            return None # Участника нет в базе
            
        # 2. Достаем сохраненный код (последние посылки)
        c.execute("""
            SELECT task_submissions 
            FROM olympiad_submissions 
            WHERE olympiad_id = ? AND participant_uuid = ?
        """, (olympiad_id, participant_uuid))
        row_sub = c.fetchone()
        
        scores = json.loads(row_res['task_scores'])
        
        # Если посылок нет (редкий случай), создаем пустой словарь
        last_submissions = {}
        if row_sub and row_sub['task_submissions']:
            last_submissions = json.loads(row_sub['task_submissions'])
            
        return {
            'scores': scores,
            'disqualified': row_res['disqualified'],
            'organization': row_res['organization'],
            'last_submissions': last_submissions
        }

# --- "Пакетная" функция ---
def _run_batch(code, test_data_list, language, judge_script, docker_image):
    """
    Выполняет "пакетную" проверку кода (1 запуск Docker на все тесты).
    Возвращает (list_of_verdicts, global_error_string)
    """
    tmp_dir = None
    try:
        tmp_dir = tempfile.mkdtemp()
        
        code_filename = "script.py" if language == "Python" else "source.cpp"
        judge_filename = "judge.py" 
        tests_filename = "tests.json"

        with open(os.path.join(tmp_dir, code_filename), "w", encoding="utf-8") as f:
            f.write(code)
        with open(os.path.join(tmp_dir, judge_filename), "w", encoding="utf-8") as f:
            f.write(judge_script)
        with open(os.path.join(tmp_dir, tests_filename), "w", encoding="utf-8") as f:
            json.dump(test_data_list, f)
            
        abs_path = os.path.abspath(tmp_dir)
        docker_path = _get_docker_path(abs_path)
        docker_volume_arg = ["-v", f"{docker_path}:/home/appuser/run:ro"]
        
        total_time_limit = sum(float(t.get('limit', 1.0)) for t in test_data_list)
        docker_total_timeout = total_time_limit + 15.0
        
        container_command = ["python3", "/home/appuser/run/judge.py"]
        command = DOCKER_COMMON_ARGS + docker_volume_arg + [docker_image] + container_command

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=docker_total_timeout
        )
        
        output = result.stdout.decode('utf-8', errors='replace')
        err = result.stderr.decode('utf-8', errors='replace')

        if err:
             return None, f"Docker/Judge Error: {err}"

        try:
            verdicts = json.loads(output)
            if isinstance(verdicts, list) and len(verdicts) > 0 and verdicts[0].get("verdict") == "Compilation Error":
                return None, verdicts[0].get("error", "Compilation Error")
            
            return verdicts, None
        
        except json.JSONDecodeError:
            return None, f"JSON Decode Error. Raw output: {output}"

    except subprocess.TimeoutExpired:
        return None, "Time Limit Exceeded (Overall Timeout)"
    except Exception as e:
        return None, f"Docker execution error: {str(e)}"
    finally:
        if tmp_dir and os.path.exists(tmp_dir):
            # Пытаемся удалить папку с повторами, так как Windows/Docker могут держать файлы
            retries = 5
            for i in range(retries):
                try:
                    shutil.rmtree(tmp_dir)
                    break # Успех, выходим из цикла
                except PermissionError:
                    if i < retries - 1:
                        time.sleep(0.2) # Ждем немного и пробуем снова
                    else:
                        print(f"WARNING: Не удалось удалить временную папку {tmp_dir} после {retries} попыток.")
                except Exception as e:
                    print(f"ERROR: Ошибка при удалении {tmp_dir}: {e}")
                    break

# --- Новая функция-обертка для Python ---
def run_python(code, test_data_list):
    """
    Принимает код и СПИСОК тестов.
    Возвращает (list_of_verdicts, global_error_string)
    """
    return _run_batch(code, test_data_list, "Python", JUDGE_SCRIPT_PYTHON, DOCKER_IMAGE_PYTHON)

# --- Новая функция-обертка для C++ ---
def run_cpp(code, test_data_list):
    """
    Принимает код и СПИСОК тестов.
    Возвращает (list_of_verdicts, global_error_string)
    """
    return _run_batch(code, test_data_list, "C++", JUDGE_SCRIPT_CPP, DOCKER_IMAGE_CPP)