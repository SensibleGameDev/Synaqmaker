import io
# ### ИЗМЕНЕНИЕ: Импортируем SocketIO и функции для работы с комнатами ###
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, send_file, abort
from flask_socketio import SocketIO, join_room, leave_room
# ---
from db_manager import DBManager, run_python, run_cpp 
import os
import time
from flask import session
import uuid 
from functools import wraps 
import configparser
import pandas as pd
from threading import Lock, Semaphore 

app = Flask(__name__)

# ### ИЗМЕНЕНИЕ: Инициализируем SocketIO ###
# async_mode='threading' хорошо работает со стандартным Flask-сервером
socketio = SocketIO(app, async_mode='threading')
# ---

@app.after_request
def add_header(response):
    """
    Запрещаем браузерам кэшировать ответы,
    чтобы студенты всегда видели последнюю версию.
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

config = configparser.ConfigParser()
if not os.path.exists('config.ini'):
    default_config = """
[security]
SECRET_KEY = "your_very_secret_key_12345_for_sessions_98765"
ADMIN_PASSWORD = "commandblock2025"

[server]
MAX_CHECKS = 20
"""
    with open('config.ini', 'w', encoding='utf-8') as f:
        f.write(default_config)
    print("WARNING: config.ini не найден. Создан файл по умолчанию. Пожалуйста, проверьте его.")

config.read('config.ini', encoding='utf-8') 

try:
    app.secret_key = config.get('security', 'SECRET_KEY').strip() 
    ADMIN_PASSWORD = config.get('security', 'ADMIN_PASSWORD').strip() 
    MAX_CONCURRENT_CHECKS = config.getint('server', 'MAX_CHECKS', fallback=20)
    print(f"INFO: Установлен лимит одновременных проверок: {MAX_CONCURRENT_CHECKS}")
    
except (configparser.NoSectionError, configparser.NoOptionError):
    print("CRITICAL ERROR: 'security' или 'server' секция не найдена. Используем defaults.")
    app.secret_key = 'fallback_secret_key'
    ADMIN_PASSWORD = 'admin'
    MAX_CONCURRENT_CHECKS = 10

if ADMIN_PASSWORD == "commandblock2025" or ADMIN_PASSWORD == "admin":
     print("WARNING: Вы используете пароль администратора по умолчанию. Обязательно смените его в config.ini")

db = DBManager()
olympiads = {}
olympiad_lock = Lock() 
docker_check_semaphore = Semaphore(MAX_CONCURRENT_CHECKS)

# ### ИЗМЕНЕНИЕ: (Вспомогательная функция) ###
def _get_olympiad_state(olympiad_id):
    """
    Собирает ПОЛНОЕ текущее состояние олимпиады.
    Это замена дублирующейся логики из старого olympiad_status
    """
    with olympiad_lock:
        if olympiad_id not in olympiads:
            return None 
        
        oly = olympiads[olympiad_id]
        
        # Расчет оставшегося времени НА СЕРВЕРЕ
        remaining_seconds = 0
        if oly['status'] == 'running' and oly.get('start_time'):
            elapsed = time.time() - oly['start_time']
            duration_sec = oly['config']['duration_minutes'] * 60
            remaining_seconds = max(0, duration_sec - elapsed)
            
            # Авто-завершение, если время вышло
            if remaining_seconds <= 0:
                oly['status'] = 'finished'

        oly_data_copy = {
            'status': oly['status'],
            'start_time': oly['start_time'],
            'config': oly['config'], 
            'participants': oly['participants'].copy(),
            'remaining_seconds': remaining_seconds  # <--- НОВОЕ ПОЛЕ
        }

    # Рассчитываем таблицу результатов (вне блокировки)
    scoreboard = []
    scoring_mode = oly_data_copy['config'].get('scoring', 'all_or_nothing')

    for p_id, p_data in oly_data_copy['participants'].items(): 
        total_score = 0
        total_penalty = 0
        
        if scoring_mode == 'icpc':
            total_score = sum(s['score'] for s in p_data['scores'].values())
            total_penalty = sum(s['penalty'] for s in p_data['scores'].values() if s['passed'])
        else:
            total_score = sum(s['score'] for s in p_data['scores'].values())
        
        scoreboard.append({
            'participant_id': p_id, 
            'nickname': p_data['nickname'],
            'organization': p_data.get('organization', None),
            'scores': p_data['scores'],
            'total_score': total_score,
            'total_penalty': total_penalty 
        })

    # Возвращаем полный пакет данных
    return {
        'status': oly_data_copy['status'],
        'remaining_seconds': oly_data_copy['remaining_seconds'], # <--- ОТПРАВЛЯЕМ
        'duration_minutes': oly_data_copy['config']['duration_minutes'],
        'config': oly_data_copy['config'], 
        'participants': [p['nickname'] for p in oly_data_copy['participants'].values()],
        'scoreboard': scoreboard
    }
# --- Конец вспомогательной функции ---


# В app.py замени ВСЮ функцию handle_join_room на эту:

@socketio.on('join_room')
def handle_join_room(data):
    room = data.get('room')
    participant_id = session.get('participant_id')
    nickname = session.get('nickname')
    session_olympiad_id = session.get('olympiad_id')
    
    print(f"DEBUG: Попытка входа. Ник: {nickname}, Комната: {room}, SessionOlyID: {session_olympiad_id}")

    if not room:
        print("ERROR: Не указана комната (room) при подключении.")
        return

    join_room(room)
    
    # Организатора просто подключаем, но не добавляем в список участников
    if session.get(f'is_organizer_for_{room}'):
        print(f"INFO: Организатор присоединился к комнате: {room}")
        current_state = _get_olympiad_state(room)
        if current_state:
            socketio.emit('full_status_update', current_state, to=request.sid)
        return

    # Логика для Участников
    with olympiad_lock:
        if room not in olympiads:
            print(f"WARNING: Участник {nickname} пытается зайти в олимпиаду {room}, которой нет в памяти (возможно, сервер был перезагружен).")
            # Можно отправить клиенту сигнал перезагрузки, но пока просто игнорируем
        else:
            oly = olympiads[room]
            
            # ПРОВЕРКА: Совпадает ли ID в сессии с комнатой?
            # Мы ослабим проверку: если participant_id есть, пробуем добавить.
            if participant_id:
                # Если участника еще нет в памяти
                if participant_id not in oly['participants']:
                    try:
                        print(f"INFO: Восстановление данных для {nickname}...")
                        # --- Блок восстановления из БД ---
                        # Оборачиваем в try-except, чтобы ошибка БД не сломала вход
                        saved_data = None
                        try:
                            saved_data = db.get_participant_progress(room, participant_id)
                        except Exception as db_err:
                            print(f"DB ERROR: Ошибка при чтении из базы: {db_err}")
                        
                        if saved_data:
                            print(f"SUCCESS: Данные из БД найдены для {nickname}.")
                            submissions_restored = saved_data.get('last_submissions', {})
                            # Дополняем ключи, если появились новые задачи
                            for tid in oly['task_ids']:
                                str_tid = str(tid)
                                if str_tid not in submissions_restored and tid not in submissions_restored:
                                    submissions_restored[str_tid] = ""
                            
                            oly['participants'][participant_id] = {
                                'nickname': nickname,
                                'organization': saved_data.get('organization') or session.get('organization'),
                                'scores': saved_data['scores'],
                                'last_submissions': submissions_restored,
                                'finished_early': False,
                                'disqualified': saved_data.get('disqualified', False),
                                'pending_submissions': 0
                            }
                        else:
                            print(f"INFO: Данных в БД нет. Создаем нового участника {nickname}.")
                            scores_data = {
                                tid: {'score': 0, 'attempts': 0, 'passed': False, 'penalty': 0} 
                                for tid in oly['task_ids']
                            }
                            oly['participants'][participant_id] = {
                                'nickname': nickname,
                                'organization': session.get('organization', None),
                                'scores': scores_data, 
                                'last_submissions': {tid: "" for tid in oly['task_ids']},
                                'finished_early': False,
                                'disqualified': False,
                                'pending_submissions': 0
                            }
                    except Exception as e:
                        print(f"CRITICAL ERROR: Ошибка при добавлении участника {nickname}: {e}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"INFO: Участник {nickname} уже есть в памяти.")
            else:
                print(f"WARNING: У {nickname} нет participant_id или не совпадает сессия. (SessID: {session_olympiad_id} != Room: {room})")

    # Отправляем состояние ВСЕМ (даже если участник не добавился, обновим тех кто есть)
    current_state = _get_olympiad_state(room)
    if current_state:
        socketio.emit('full_status_update', current_state, to=room)
@app.route('/')
def index():
    if not session.get('is_admin'):
        return redirect(url_for('olympiad_index'))
    tasks = db.get_tasks()
    return render_template('index.html', tasks=tasks)


# Управление задачами (CRUD)
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_admin'):
            flash('Доступ запрещен. Пожалуйста, войдите как администратор.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/run_code', methods=['POST'])
def run_code_submission():
    data = request.json
    task_id = int(data['task_id'])
    language = data['language']
    code = data['code']

    tests = db.get_tests_for_task(task_id)
    if not tests:
        return jsonify({'error': 'Нет тестов для этой задачи'}), 400

    # --- ИЗМЕНЕНИЕ: Готовим данные для "пакетной" проверки ---
    test_data_list = [
        {
            'input': t['test_input'].replace('\r\n', '\n') if t['test_input'] else '',
            'output': t['expected_output'].replace('\r\n', '\n') if t['expected_output'] else '',
            'limit': t['time_limit']
        } for t in tests
    ]
    
    runner = run_python if language == "Python" else run_cpp
    
    # --- ИЗМЕНЕНИЕ: Вызываем runner ОДИН РАЗ ---
    verdicts, global_err = runner(code, test_data_list)
    
    results = []
    passed_count = 0

    if global_err:
        # Глобальная ошибка (CE, Judge Error, Global TL)
        verdict = "Compilation Error" if "Compilation Error" in global_err else "Runtime Error"
        # Заполняем все тесты этой ошибкой
        for i, t in enumerate(tests):
            results.append({
                'test_num': i + 1,
                'verdict': verdict,
                'input': t['test_input'],
                'expected': t['expected_output'],
                'output': '',
                'error': global_err,
                'passed': False
            })
    else:
        # --- ИЗМЕНЕНИЕ: Обрабатываем "пакетный" результат ---
        for i, v in enumerate(verdicts):
            verdict = v.get('verdict', 'Internal Error')
            passed = (verdict == "Accepted")
            if passed:
                passed_count += 1
                
            results.append({
                'test_num': i + 1,
                'verdict': verdict,
                'input': tests[i]['test_input'],
                'expected': tests[i]['expected_output'],
                'output': v.get('output', ''),
                'error': v.get('error', ''),
                'passed': passed
            })

    overall_result = {
        'passed_count': passed_count,
        'total_tests': len(tests),
        'details': results
    }
    
    return jsonify(overall_result)
    
# РЕЖИМ ОЛИМПИАДЫ 

@app.route('/olympiad/submit/<olympiad_id>', methods=['POST'])
def olympiad_submit(olympiad_id):
    participant_id = session.get('participant_id')
    oly_config = None
    task_submissions_info = None

    data = request.json
    task_id = int(data['task_id'])
    language = data['language']
    code = data['code']
    
    with olympiad_lock: 
        if olympiad_id not in olympiads or not participant_id:
            return jsonify({'error': 'Олимпиада не активна или вы не участник.'}), 403

        oly = olympiads[olympiad_id]
        
        if participant_id not in oly['participants']:
            return jsonify({'error': 'Участник не найден в этой олимпиаде.'}), 403

        p_data = oly['participants'][participant_id] 
        oly_config = oly['config'].copy() 
        scoring_mode = oly_config.get('scoring', 'all_or_nothing')

        if p_data.get('disqualified'):
             return jsonify({'error': 'Вы были дисквалифицированы.'}), 400
        if p_data.get('finished_early'):
             return jsonify({'error': 'Вы уже завершили олимпиаду.'}), 400
        if oly.get('start_time'):
            elapsed = time.time() - oly['start_time']
            if elapsed > oly['config']['duration_minutes'] * 60:
                return jsonify({'error': 'Время вышло!'}), 400
        
        # --- НОВАЯ ПРОВЕРКА: Ограничение на 3 одновременных посылки ---
        if p_data.get('pending_submissions', 0) >= 3:
            return jsonify({'error': 'Слишком много одновременных проверок. Подождите, пока завершатся предыдущие.'}), 429 # 429 Too Many Requests
        
        p_data['pending_submissions'] = p_data.get('pending_submissions', 0) + 1
        print(f"INFO: Участник {participant_id} отправил посылку. В очереди: {p_data['pending_submissions']}")
        # --- КОНЕЦ НОВОЙ ПРОВЕРКИ ---

        p_data['last_submissions'][task_id] = code 
        
        task_submissions = p_data['scores'][task_id]
        
        if scoring_mode in ['all_or_nothing', 'icpc'] and task_submissions.get('passed'):
            # --- ИЗМЕНЕНИЕ: Уменьшаем счетчик, т.к. проверка не будет запущена ---
            p_data['pending_submissions'] = max(0, p_data.get('pending_submissions', 1) - 1)
            return jsonify({'error': 'Задача уже решена.'}), 400

        task_submissions_info = task_submissions.copy()
        
    tests = db.get_tests_for_task(task_id)
    if not tests:
        # --- ИЗМЕНЕНИЕ: Уменьшаем счетчик, т.к. проверка не будет запущена ---
        with olympiad_lock:
             if olympiad_id in olympiads and participant_id in olympiads[olympiad_id]['participants']:
                 oly = olympiads[olympiad_id]
                 p_data = oly['participants'][participant_id]
                 p_data['pending_submissions'] = max(0, p_data.get('pending_submissions', 1) - 1)
        return jsonify({'error': 'Тесты для задачи не найдены.'}), 404
        
    # ### ИЗМЕНЕНИЕ: Отправляем PENDING-статус всем в комнате ###
    socketio.emit('submission_pending', {
        'participant_id': participant_id,
        'task_id': task_id
    }, to=olympiad_id)
    # ---

    # --- ИЗМЕНЕНИЕ: Готовим "пакет" тестов ---
    test_data_list = [
        {
            'input': t['test_input'].replace('\r\n', '\n') if t['test_input'] else '',
            'output': t['expected_output'].replace('\r\n', '\n') if t['expected_output'] else '',
            'limit': t['time_limit']
        } for t in tests
    ]
        
    runner = run_python if language == "Python" else run_cpp
    
    # --- НОВЫЙ БЛОК: try...finally для ГАРАНТИРОВАННОГО уменьшения счетчика ---
    try:
        results_details = []
        passed_count = 0
        is_correct = False 
        
        verdicts = None
        global_err = None

        print(f"INFO: Участник {participant_id} ждет СЕМАФОР для задачи {task_id}")
        with docker_check_semaphore:
            print(f"INFO: Участник {participant_id} получил СЕМАФОР. Начинаем проверку...")
            
            verdicts, global_err = runner(code, test_data_list)
            
            print(f"INFO: Участник {participant_id} завершил проверку. СЕМАФОР ОСВОБОЖДЕН.")
        
        if global_err:
            verdict = "Compilation Error" if "Compilation Error" in global_err else "Runtime Error"
            results_details.append({'test_num': 1, 'verdict': verdict, 'error': global_err})
        else:
            for i, v in enumerate(verdicts):
                verdict = v.get('verdict', 'Internal Error')
                results_details.append({'test_num': i + 1, 'verdict': verdict})
                
                if verdict == "Accepted":
                    passed_count += 1
                elif verdict not in ["Accepted", "Wrong Answer"]:
                    break
        
        is_correct = (passed_count == len(tests)) and (not global_err)
        
        new_score_info = {}
        
        with olympiad_lock:
            if olympiad_id not in olympiads:
                 return jsonify({'error': 'Олимпиада завершилась во время проверки.'}), 400
            
            oly = olympiads[olympiad_id]
            
            if oly['status'] != 'running':
                 return jsonify({'error': 'Олимпиада завершилась во время проверки.'}), 400
                 
            p_data = oly['participants'][participant_id]
            
            if p_data.get('disqualified'):
                 return jsonify({'error': 'Вас дисквалифицировали во время проверки.'}), 400

            task_submissions = p_data['scores'][task_id]
            scoring_mode = oly_config.get('scoring', 'all_or_nothing')
            
            if scoring_mode == 'icpc':
                if not task_submissions['passed']: 
                    if is_correct:
                        task_submissions['passed'] = True
                        task_submissions['score'] = 1 
                        elapsed_seconds = time.time() - oly['start_time']
                        solve_time_minutes = int(elapsed_seconds / 60)
                        penalty_attempts = task_submissions_info['attempts'] * 20
                        task_submissions['penalty'] = solve_time_minutes + penalty_attempts
                    elif not global_err: 
                        task_submissions['attempts'] += 1

            elif scoring_mode == 'per_test':
                if passed_count > task_submissions['score']:
                    task_submissions['score'] = passed_count
                if not is_correct and not global_err: 
                    task_submissions['attempts'] += 1
                if is_correct:
                    task_submissions['passed'] = True

            else: # 'all_or_nothing'
                if is_correct:
                    task_submissions['score'] = 100
                    task_submissions['passed'] = True
                elif not global_err: 
                    task_submissions['attempts'] += 1
            
            new_score_info = task_submissions.copy()

        # Возвращаем результат только *этому* участнику
        participant_response = {
            'passed_count': passed_count,
            'total_tests': len(tests),
            'new_score': new_score_info.get('score', 0),
            'attempts': new_score_info.get('attempts', 0),
            'penalty': new_score_info.get('penalty', 0),
            'passed': new_score_info.get('passed', False),
            'details': results_details
        }
        
        return jsonify(participant_response)
        
    finally:
        # --- НОВЫЙ БЛОК: Уменьшаем счетчик в любом случае ---
        with olympiad_lock:
            if olympiad_id in olympiads and participant_id in olympiads[olympiad_id]['participants']:
                oly = olympiads[olympiad_id]
                p_data = oly['participants'][participant_id]
                p_data['pending_submissions'] = max(0, p_data.get('pending_submissions', 1) - 1)
                
                # !!! ДОБАВИТЬ ВОТ ЭТО !!!
                # Сохраняем промежуточный результат, чтобы не потерять данные при краше
                try:
                    # Внимание: это может быть чуть медленно, но безопасно
                    # Можно оптимизировать, сохраняя только если is_correct или прошли тесты
                    db.save_olympiad_data(olympiad_id, oly) 
                except Exception as e:
                    print(f"ERROR: Ошибка автосохранения: {e}")
        # ### ИЗМЕНЕНИЕ: Отправляем ОБНОВЛЕННОЕ СОСТОЯНИЕ всем в комнате ###
        current_state = _get_olympiad_state(olympiad_id)
        if current_state:
            socketio.emit('full_status_update', current_state, to=olympiad_id)
        # ---
    # --- КОНЕЦ БЛОКА try...finally ---

@app.route('/olympiad')
def olympiad_index():
    active_olympiads = {}
    if session.get('is_admin'):
        with olympiad_lock:
            active_olympiads = {
                oid: odata for oid, odata in olympiads.items()
                if odata.get('status') in ['waiting', 'running']
            }
    return render_template('olympiad_index.html', active_olympiads=active_olympiads)


@app.route('/olympiad/create', methods=['GET', 'POST'])
@admin_required
def olympiad_create():
    if request.method == 'POST':
        task_ids = request.form.getlist('task_ids')
        duration = int(request.form.get('duration'))
        scoring = request.form.get('scoring')
        mode = request.form.get('mode') 

        if not (1 <= len(task_ids) <= 10):
            flash('Необходимо выбрать от 1 до 10 задач.', 'danger') 
            return redirect(url_for('olympiad_create'))
        task_ids.sort(key=int) 

        olympiad_id = str(uuid.uuid4())[:8] 

        with olympiad_lock:
            while olympiad_id in olympiads:
                olympiad_id = str(uuid.uuid4())[:8] 

            olympiads[olympiad_id] = {
                'status': 'waiting',
                'task_ids': [int(tid) for tid in task_ids],
                'tasks_details': [db.get_task_details(tid) for tid in task_ids],
                'config': {
                    'duration_minutes': duration,
                    'scoring': scoring,
                    'mode': mode 
                },
                'start_time': None,
                'participants': {} 
            }
        return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))

    tasks = db.get_tasks()
    return render_template('olympiad_create.html', tasks=tasks)

@app.route('/olympiad/mode/<olympiad_id>')
def get_olympiad_mode(olympiad_id):
    """API: Возвращает режим олимпиады (free/closed) для UI."""

    with olympiad_lock:
        if olympiad_id not in olympiads:
            return jsonify({'error': 'not found'}), 404
        
        mode = olympiads[olympiad_id].get('config', {}).get('mode', 'free')
    
    return jsonify({'mode': mode})


@app.route('/olympiad/join', methods=['GET', 'POST'])
def olympiad_join():
    if request.method == 'POST':
        nickname = request.form.get('nickname', '').strip()
        olympiad_id = request.form.get('olympiad_id', '').strip()
        password = request.form.get('password', '').strip() 

        if not nickname or not olympiad_id:
            flash('Нужно ввести и никнейм, и ID олимпиады.', 'warning')
            return redirect(url_for('olympiad_join'))
        
        oly_data_copy = None 

        with olympiad_lock:
            if olympiad_id not in olympiads:
                flash('Олимпиада с таким ID не найдена.', 'danger')
                return redirect(url_for('olympiad_join'))

            oly = olympiads[olympiad_id]
            oly_data_copy = {
                'config': oly['config'],
                'status': oly['status'],
                'participants': oly['participants'].copy() 
            }

        mode = oly_data_copy['config'].get('mode', 'free')

        participant_id_to_set = None
        participant_org = None

        if mode == 'free':

            existing_participant_id = None
            for p_id, p_data in oly_data_copy['participants'].items():
                if p_data['nickname'] == nickname:
                    existing_participant_id = p_id
                    if p_data.get('finished_early'):
                        flash('Вы уже завершили эту олимпиаду и не можете переподключиться.', 'warning')
                        return redirect(url_for('olympiad_end', olympiad_id=olympiad_id))
                    if p_data.get('disqualified'):
                        flash('Вы были дисквалифицированы с этой олимпиады.', 'danger')
                        return redirect(url_for('olympiad_end', olympiad_id=olympiad_id))
                    
                    break
            
            if existing_participant_id:
                participant_id_to_set = existing_participant_id
            else:
                participant_id_to_set = str(uuid.uuid4())
        
        else:

            if not password:
                flash('Это закрытая олимпиада. Необходимо ввести пароль.', 'warning')
                return redirect(url_for('olympiad_join'))

            participant_data = db.validate_closed_participant(olympiad_id, nickname, password)
            
            if not participant_data:
                flash('Неверный никнейм или пароль для этой олимпиады.', 'danger')
                return redirect(url_for('olympiad_join'))
                
            participant_db_id = str(participant_data['id']) 
            participant_org = participant_data['organization']

            if participant_db_id in oly_data_copy['participants'] and oly_data_copy['participants'][participant_db_id].get('finished_early'):
                flash('Вы уже завершили эту олимпиаду и не можете переподключиться.', 'warning')
                return redirect(url_for('olympiad_end', olympiad_id=olympiad_id))
            
            participant_id_to_set = participant_db_id

        session['participant_id'] = participant_id_to_set
        session['nickname'] = nickname
        session['olympiad_id'] = olympiad_id
        if participant_org:
            session['organization'] = participant_org
        
        if oly_data_copy.get('status') == 'running':
            return redirect(url_for('olympiad_run', olympiad_id=olympiad_id))
        else:
            return redirect(url_for('olympiad_lobby', olympiad_id=olympiad_id))

    return render_template('olympiad_join.html')


@app.route('/olympiad/lobby/<olympiad_id>')
def olympiad_lobby(olympiad_id):
    nickname = session.get('nickname')
    if not nickname or session.get('olympiad_id') != olympiad_id:
        return redirect(url_for('olympiad_join'))
    return render_template('olympiad_lobby.html', olympiad_id=olympiad_id, nickname=nickname)

@app.route('/olympiad/host/<olympiad_id>')
@admin_required
def olympiad_host(olympiad_id):
    # --- ИСПРАВЛЕНИЕ БАГА №2 (Дополнительная защита) ---
    # Если мы заходим как хост, удаляем из сессии данные участника,
    # чтобы случайно не зарегистрироваться в своей же олимпиаде.
    session.pop('participant_id', None)
    session.pop('nickname', None)
    session.pop('olympiad_id', None)
    session.pop('organization', None)
    # ---------------------------------------------------
    oly_data_copy = None
    oly_mode = 'free'
    tasks_details = []
    
    with olympiad_lock:
        if olympiad_id not in olympiads:
            return "Олимпиада не найдена", 404
        
        session[f'is_organizer_for_{olympiad_id}'] = True
        
        oly_data = olympiads[olympiad_id]
        oly_mode = oly_data['config'].get('mode', 'free')
        tasks_details = oly_data['tasks_details']
        oly_data_copy = oly_data.copy()
        
    whitelist = []
    
    if oly_mode == 'closed':
        whitelist = db.get_whitelist_for_olympiad(olympiad_id)
        
    return render_template('olympiad_host.html', 
                           olympiad_id=olympiad_id, 
                           tasks=tasks_details,
                           oly_mode=oly_mode,
                           whitelist=whitelist,
                           olympiad_data=oly_data_copy)


@app.route('/olympiad/start/<olympiad_id>', methods=['POST'])
@admin_required
def olympiad_start(olympiad_id):

    with olympiad_lock:
        if olympiad_id in olympiads:
            olympiads[olympiad_id]['status'] = 'running'
            olympiads[olympiad_id]['start_time'] = time.time()
            
            # ### ИЗМЕНЕНИЕ: Отправляем "СТАРТ" всем в комнате ###
            socketio.emit('olympiad_started', {'status': 'ok'}, to=olympiad_id)
            # ---
            return jsonify({'status': 'ok'})
    return jsonify({'status': 'error'}), 404

@app.route('/olympiad/run/<olympiad_id>')
def olympiad_run(olympiad_id):
    
    oly_data_copy = None
    participant_data_copy = None
    with olympiad_lock:
        # --- ИСПРАВЛЕНИЕ БАГА №1 ---
        if session.get('olympiad_id') != olympiad_id:
            # Сессия этого пользователя - от ДРУГОЙ олимпиады. Сбрасываем.
            flash('Вы вошли в другую олимпиаду. Войдите заново.', 'warning')
            session.pop('participant_id', None)
            session.pop('nickname', None)
            session.pop('olympiad_id', None)
            session.pop('organization', None)
            return redirect(url_for('olympiad_join'))
    with olympiad_lock:
        if olympiad_id not in olympiads or 'nickname' not in session:
            return redirect(url_for('olympiad_join'))
        
        oly = olympiads[olympiad_id]
        participant_id = session.get('participant_id') 
        participant_data = oly['participants'].get(participant_id, {})
        
        
        if participant_data.get('finished_early'):
            flash('Вы уже завершили эту олимпиаду.', 'info')
            return redirect(url_for('olympiad_end', olympiad_id=olympiad_id))
        if participant_data.get('disqualified'):
            flash('Вы были дисквалифицированы.', 'danger')
            return redirect(url_for('olympiad_end', olympiad_id=olympiad_id))
        if oly['status'] != 'running':
            return redirect(url_for('olympiad_lobby', olympiad_id=olympiad_id))

        if participant_id and participant_id not in oly['participants']:
            

            scores_data = {
                tid: {
                    'score': 0,      # Баллы (или 1/0 для ICPC)
                    'attempts': 0,   # Неверные попытки
                    'passed': False, # Решена ли (True/False)
                    'penalty': 0     # Штраф в минутах (только для ICPC)
                } for tid in oly['task_ids']
            }
            
            oly['participants'][participant_id] = {
                'nickname': session['nickname'],
                'organization': session.get('organization', None), 
                'scores': scores_data, 
                'last_submissions': {tid: "" for tid in oly['task_ids']},
                'finished_early': False,
                'disqualified': False, # <--- Добавил на всякий случай
                'pending_submissions': 0  # <--- ДОБАВЛЕНА ЭТА СТРОКА
            }

        
        oly_data_copy = oly.copy()
        
    return render_template('olympiad_run.html', 
                           olympiad_id=olympiad_id, 
                           oly_session=oly_data_copy, 
                           participant_id=participant_id)

@app.route('/olympiad/finish_early/<olympiad_id>', methods=['POST'])
def olympiad_finish_early(olympiad_id):

    with olympiad_lock:
        if olympiad_id not in olympiads or 'participant_id' not in session:
            return redirect(url_for('olympiad_join'))
        
        participant_id = session['participant_id']
        oly = olympiads[olympiad_id]

        if participant_id in oly['participants']:
            oly['participants'][participant_id]['finished_early'] = True
            flash('Вы успешно завершили олимпиаду.', 'success')
    
    # ### ИЗМЕНЕНИЕ: Отправляем обновление, т.к. участник "завершил" ###
    current_state = _get_olympiad_state(olympiad_id)
    if current_state:
        socketio.emit('full_status_update', current_state, to=olympiad_id)
    # ---
    
    return redirect(url_for('olympiad_end', olympiad_id=olympiad_id))

@app.route('/olympiad/end/<olympiad_id>')
def olympiad_end(olympiad_id):
    
    results_copy = None
    
    with olympiad_lock:
        if olympiad_id in olympiads:
            results_copy = olympiads[olympiad_id].copy()

    if results_copy:
        results = results_copy
        participants_list = []
        scoring_mode = results.get('config', {}).get('scoring', 'all_or_nothing')

        for p_id, p_data in results['participants'].items():
            

            total_score = 0
            total_penalty = 0
            
            if scoring_mode == 'icpc':
                total_score = sum(s['score'] for s in p_data['scores'].values()) # Кол-во решенных
                total_penalty = sum(s['penalty'] for s in p_data['scores'].values() if s['passed'])
            else:
                total_score = sum(s['score'] for s in p_data['scores'].values()) # Сумма баллов


            normalized_scores = {str(k): v for k, v in p_data['scores'].items()}
            
            participants_list.append({
                'nickname': p_data['nickname'],
                'organization': p_data.get('organization', None), 
                'scores': normalized_scores, 
                'total_score': total_score,
                'total_penalty': total_penalty, 
                'disqualified': p_data.get('disqualified', False)
            })
        

        if scoring_mode == 'icpc':
            participants_list.sort(key=lambda p: (p['total_score'], -p['total_penalty']), reverse=True)
        else:
            participants_list.sort(key=lambda p: p['total_score'], reverse=True)
        tasks_details = results['tasks_details']
        
    else:
        db_results = db.get_olympiad_results(olympiad_id)
        if not db_results:
             return "Олимпиада не найдена", 404
        
        results = db_results['results']
        tasks_details = db_results['tasks']
        participants_list = db_results['participants_list']

    is_organizer = session.get(f'is_organizer_for_{olympiad_id}', False)
    
    return render_template(
        'olympiad_end.html', 
        results=results, 
        tasks=tasks_details,
        participants_list=participants_list,
        is_organizer=is_organizer,
        olympiad_id=olympiad_id
    )
    

# ### ИЗМЕНЕНИЕ: Этот HTTP-маршрут больше не нужен для опроса ###
# Клиенты получают обновления по WebSocket.
# Оставим его для отладки или для API, если понадобится.
@app.route('/olympiad/status/<olympiad_id>')
def olympiad_status(olympiad_id):
    print("DEBUG: /olympiad/status/ был вызван (HTTP)")
    state = _get_olympiad_state(olympiad_id)
    if state:
        return jsonify(state)
    else:
        return jsonify({'error': 'not found'}), 404
# ---
    

@app.route('/olympiad/host/<olympiad_id>/disqualify/<participant_id>', methods=['POST'])
@admin_required
def olympiad_disqualify(olympiad_id, participant_id):
    """ОРГАНИЗАТОР: Дисквалифицирует участника."""
    
    nickname = "???"

    with olympiad_lock:
        if olympiad_id not in olympiads:
            return "Олимпиада не найдена", 404
            
        oly = olympiads[olympiad_id]
        
        if participant_id in oly['participants']:
            p_data = oly['participants'][participant_id]
            p_data['disqualified'] = True 
            p_data['finished_early'] = True 
            nickname = p_data['nickname']
            
            for task_id in p_data['scores']:
                 p_data['scores'][task_id]['score'] = 0 
                
            flash(f"Участник {nickname} был дисквалифицирован. Все баллы обнулены.", 'warning')
        else:
            flash('Участник не найден.', 'danger')

    # ### ИЗМЕНЕНИЕ: Отправляем обновление, т.к. участник DQ ###
    current_state = _get_olympiad_state(olympiad_id)
    if current_state:
        socketio.emit('full_status_update', current_state, to=olympiad_id)
    # ---
        
    return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))

@app.route('/olympiad/finish_by_host/<olympiad_id>', methods=['POST'])
@admin_required
def olympiad_finish_by_host(olympiad_id):
    
    oly_data_to_save = None
    

    with olympiad_lock:
        if olympiad_id in olympiads:
            olympiads[olympiad_id]['status'] = 'finished' 
            oly_data_to_save = olympiads[olympiad_id].copy()
            session.pop(f'is_organizer_for_{olympiad_id}', None)

            del olympiads[olympiad_id] 

    # ### ИЗМЕНЕНИЕ: Сообщаем всем, что олимпиада завершена ###
    socketio.emit('olympiad_finished', {'status': 'finished'}, to=olympiad_id)
    # ---
    
    if oly_data_to_save:
        db.save_olympiad_data(olympiad_id, oly_data_to_save)
        return jsonify({'status': 'ok', 'message': 'Олимпиада завершена.'})
        
    return jsonify({'status': 'error'}), 404

@app.route('/tasks/<int:task_id>/tests')
@admin_required
def tests_list(task_id):
    task = db.get_task_details(task_id)
    tests = db.get_tests_for_task(task_id)
    return render_template('tests.html', tests=tests, task=task)

@app.route('/tasks/<int:task_id>/tests/add', methods=['GET', 'POST'])
@admin_required
def add_test(task_id):
    if request.method == 'POST':
        test_input = request.form['test_input']
        expected_output = request.form['expected_output']
        time_limit = float(request.form.get('time_limit', 1.0))
        db.add_test(task_id, test_input, expected_output, time_limit)
        flash('Тест успешно добавлен!', 'success')
        return redirect(url_for('tests_list', task_id=task_id))
    
    task = db.get_task_details(task_id)
    return render_template('test_form.html', title="Добавить тест", task=task)

@app.route('/tasks/<int:task_id>/tests/edit/<int:test_id>', methods=['GET', 'POST'])
@admin_required
def edit_test(task_id, test_id):
    test = db.get_test_details(test_id)
    if request.method == 'POST':
        test_input = request.form['test_input']
        expected_output = request.form['expected_output']
        time_limit = float(request.form.get('time_limit', 1.0))
        db.update_test(test_id, test_input, expected_output, time_limit)
        flash('Тест успешно обновлен!', 'success')
        return redirect(url_for('tests_list', task_id=task_id))
        
    task = db.get_task_details(task_id)
    return render_template('test_form.html', title="Редактировать тест", task=task, test=test)

@app.route('/tasks/<int:task_id>/tests/delete/<int:test_id>', methods=['POST'])
@admin_required
def delete_test(task_id, test_id):
    db.delete_test(test_id)
    flash('Тест удален.', 'info')
    return redirect(url_for('tests_list', task_id=task_id))

@app.route('/tasks/<int:task_id>/tests/import_excel', methods=['POST'])
@admin_required
def import_tests_from_excel(task_id):
    
    if 'tests_file' not in request.files:
        flash('Файл не найден.', 'danger')
        return redirect(url_for('tests_list', task_id=task_id))
        
    file = request.files['tests_file']
    if file.filename == '':
        flash('Файл не выбран.', 'danger')
        return redirect(url_for('tests_list', task_id=task_id))

    default_time_limit = float(request.form.get('time_limit_excel', 1.0))

    if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        try:
            df = pd.read_excel(file, header=None) 
            
            if len(df.columns) < 2:
                flash('Ошибка формата: Ожидается 2 колонки (Ввод, Вывод).', 'danger')
                return redirect(url_for('tests_list', task_id=task_id))

            added_count = 0
            for index, row in df.iterrows():
                test_input = str(row.iloc[0])
                expected_output = str(row.iloc[1])
                
                if not test_input and not expected_output:
                    continue
                    
                db.add_test(task_id, test_input, expected_output, default_time_limit)
                added_count += 1
                    
            flash(f'Импорт завершен: {added_count} тестов успешно добавлено.', 'success')

        except Exception as e:
            flash(f'Ошибка при чтении файла Excel: {e}', 'danger')
    else:
        flash('Неверный формат файла. Нужен .xlsx или .xls', 'danger')
            
    return redirect(url_for('tests_list', task_id=task_id))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password')
        if password == ADMIN_PASSWORD:
            session['is_admin'] = True
            flash('Вы успешно вошли в систему!', 'success')
            return redirect(url_for('tasks_list'))
        else:
            flash('Неверный пароль.', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('Вы вышли из системы.', 'info')
    return redirect(url_for('index'))


@app.route('/tasks')
@admin_required
def tasks_list():
    tasks = db.get_tasks()
    return render_template('tasks.html', tasks=tasks)

@app.route('/tasks/add', methods=['GET', 'POST'])
@admin_required
def add_task():
    if request.method == 'POST':
        title = request.form['title']
        difficulty = request.form['difficulty']
        topic = request.form['topic']
        description = request.form['description']
        
        attachment = None
        file_format = None
        if 'attachment' in request.files:
            file = request.files['attachment']
            if file.filename != '':
                attachment = file.read()
                file_format = os.path.splitext(file.filename)[1].lower()

        if not title:
            flash('Название задачи не может быть пустым!', 'danger')
        else:
            db.add_task(title, difficulty, topic, description, attachment, file_format)
            flash('Задача успешно добавлена!', 'success')
            return redirect(url_for('tasks_list'))
            
    return render_template('task_form.html', title="Добавить задачу")

@app.route('/tasks/edit/<int:task_id>', methods=['GET', 'POST'])
@admin_required
def edit_task(task_id):
    task = db.get_task_details(task_id)
    if request.method == 'POST':
        title = request.form['title']
        difficulty = request.form['difficulty']
        topic = request.form['topic']
        description = request.form['description']

        attachment = None
        file_format = None
        if 'attachment' in request.files:
            file = request.files['attachment']
            if file.filename != '':
                attachment = file.read()
                file_format = os.path.splitext(file.filename)[1].lower()
        
        db.update_task(task_id, title, difficulty, topic, description, attachment, file_format)
        flash('Задача успешно обновлена!', 'success')
        return redirect(url_for('tasks_list'))

    return render_template('task_form.html', title="Редактировать задачу", task=task)

@app.route('/tasks/delete/<int:task_id>', methods=['POST'])
@admin_required
def delete_task(task_id):
    db.delete_task(task_id)
    flash('Задача и все связанные с ней тесты удалены.', 'info')
    return redirect(url_for('tasks_list'))

@app.route('/tasks/view/<int:task_id>')
def view_task(task_id):
    task = db.get_task_details(task_id)
    if not task:
        abort(404) 
    tests = db.get_tests_for_task(task_id)
    return render_template('view_task.html', task=task, tests=tests)
    
@app.route('/tasks/<int:task_id>/attachment')
def display_attachment(task_id):
    task_data = db.get_task_details(task_id)
    if task_data and task_data[5]:
        attachment_data = task_data[5]
        file_format = task_data[6] or '' 

        mimetype = 'application/octet-stream' 
        if file_format == '.pdf':
            mimetype = 'application/pdf'
        elif file_format == '.html':
            mimetype = 'text/html'
        
        return send_file(io.BytesIO(attachment_data), mimetype=mimetype)
        
    return "Файл не найден", 404

@app.route('/olympiad/host/<olympiad_id>/add_participant', methods=['POST'])
@admin_required
def olympiad_add_participant(olympiad_id):

    if olympiad_id not in olympiads: 
        return "Олимпиада не найдена", 404

    nickname = request.form.get('nickname').strip()
    organization = request.form.get('organization').strip()
    password = request.form.get('password').strip()
    
    if not nickname or not password:
        flash('Никнейм и пароль обязательны.', 'danger')
        return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))
        
    success, message = db.add_participant_to_whitelist(olympiad_id, nickname, organization, password)
    
    if success:
        flash(message, 'success')
    else:
        flash(message, 'danger')
        
    return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))


@app.route('/olympiad/host/<olympiad_id>/remove_participant/<int:participant_db_id>', methods=['POST'])
@admin_required
def olympiad_remove_participant(olympiad_id, participant_db_id):

    if olympiad_id not in olympiads:
        return "Олимпиада не найдена", 404
    
    if db.remove_participant_from_whitelist(participant_db_id):
        flash('Участник удален.', 'success')
    else:
        flash('Не удалось удалить участника.', 'danger')
        
    return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))


@app.route('/olympiad/host/<olympiad_id>/upload_participants', methods=['POST'])
@admin_required
def olympiad_upload_participants(olympiad_id): 
    if olympiad_id not in olympiads:
        return "Олимпиада не найдена", 404
    if 'participant_file' not in request.files:
        flash('Файл не найден.', 'danger')
        return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))
        
    file = request.files['participant_file']
    if file.filename == '':
        flash('Файл не выбран.', 'danger')
        return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))

    if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        try:
            df = pd.read_excel(file, header=None) # Убран header
            
            if len(df.columns) < 3:
                flash('Ошибка формата: Ожидается 3 колонки (Никнейм, Организация, Пароль).', 'danger')
                return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))

            added_count = 0
            errors_count = 0
            
            for index, row in df.iterrows():
                nickname = str(row.iloc[0]).strip()
                organization = str(row.iloc[1]).strip()
                password = str(row.iloc[2]).strip()
                
                success, message = db.add_participant_to_whitelist(olympiad_id, nickname, organization, password)
                if success:
                    added_count += 1
                else:
                    errors_count += 1
                    
            flash(f'Импорт завершен: {added_count} участников добавлено, {errors_count} ошибок (возможно, дубликаты).', 'info')

        except Exception as e:
            flash(f'Ошибка при чтении файла Excel: {e}', 'danger')
    else:
        flash('Неверный формат файла. Нужен .xlsx или .xls', 'danger')
            
    return redirect(url_for('olympiad_host', olympiad_id=olympiad_id))