import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
import json
import pandas as pd
import re

DB_NAME = "testirovschik.db"

class ResultsViewer(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Просмотр результатов олимпиад")
        self.geometry("1200x800")

        # --- Структура окна ---
        main_pane = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True)

        # Левая панель: Список олимпиад
        left_frame = ttk.Frame(main_pane, padding=10)
        main_pane.add(left_frame, weight=1)

        # Правая панель: Результаты и код
        right_pane = ttk.PanedWindow(main_pane, orient=tk.VERTICAL)
        main_pane.add(right_pane, weight=4)
        
        results_frame = ttk.Frame(right_pane, padding=10)
        code_frame = ttk.Frame(right_pane, padding=10)
        right_pane.add(results_frame, weight=2)
        right_pane.add(code_frame, weight=3)

        # --- Левая панель ---
        ttk.Label(left_frame, text="Завершенные олимпиады", font=("Helvetica", 12, "bold")).pack(pady=5)
        self.olympiad_list = tk.Listbox(left_frame, font=("Courier", 11))
        self.olympiad_list.pack(fill=tk.BOTH, expand=True)
        self.olympiad_list.bind('<<ListboxSelect>>', self.on_olympiad_select)
        
        # Контекстное меню
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="Удалить олимпиаду", command=self.delete_selected_olympiad)
        self.olympiad_list.bind("<Button-3>", self.show_context_menu)

        # Кнопка экспорта
        export_button = ttk.Button(left_frame, text="Экспорт в Excel", command=self.export_to_excel)
        export_button.pack(pady=10, fill=tk.X)

        # --- Правая панель (Таблица) ---
        ttk.Label(results_frame, text="Таблица результатов", font=("Helvetica", 12, "bold")).pack(pady=5)
        # Создаем дерево с прокруткой
        self.tree_scroll = ttk.Scrollbar(results_frame)
        self.tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.results_tree = ttk.Treeview(results_frame, yscrollcommand=self.tree_scroll.set, show='headings')
        self.tree_scroll.config(command=self.results_tree.yview)
        
        self.results_tree.pack(fill=tk.BOTH, expand=True)
        self.results_tree.bind('<<TreeviewSelect>>', self.on_participant_select)

        # --- Правая панель (Код) ---
        ttk.Label(code_frame, text="Последние решения", font=("Helvetica", 12, "bold")).pack(pady=5)
        self.task_notebook = ttk.Notebook(code_frame)
        self.task_notebook.pack(fill=tk.BOTH, expand=True)

        self.selected_olympiad_id = None
        self.load_olympiads()

    def db_connect(self):
        return sqlite3.connect(DB_NAME)

    def load_olympiads(self):
        self.olympiad_list.delete(0, tk.END)
        try:
            conn = self.db_connect()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT olympiad_id FROM olympiad_results ORDER BY id DESC")
            olympiads = cursor.fetchall()
            conn.close()
            for oly in olympiads:
                self.olympiad_list.insert(tk.END, oly[0])
        except Exception as e:
            self.olympiad_list.insert(tk.END, f"Ошибка БД: {e}")

    def delete_selected_olympiad(self):
        selection = self.olympiad_list.curselection()
        if not selection: return
        olympiad_id = self.olympiad_list.get(selection[0])
        
        if not messagebox.askyesno("Подтверждение", f"Удалить олимпиаду '{olympiad_id}' навсегда?"):
            return

        try:
            conn = self.db_connect()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM olympiad_results WHERE olympiad_id = ?", (olympiad_id,))
            cursor.execute("DELETE FROM olympiad_submissions WHERE olympiad_id = ?", (olympiad_id,))
            conn.commit()
            conn.close()
            
            self.load_olympiads()
            self.results_tree.delete(*self.results_tree.get_children())
            for tab in self.task_notebook.tabs(): self.task_notebook.forget(tab)
            messagebox.showinfo("Успех", "Удалено.")
        except Exception as e:
            messagebox.showerror("Ошибка", str(e))

    def show_context_menu(self, event):
        try:
            self.olympiad_list.selection_clear(0, tk.END)
            self.olympiad_list.selection_set(self.olympiad_list.nearest(event.y))
            self.olympiad_list.activate(self.olympiad_list.nearest(event.y))
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()

    def on_olympiad_select(self, event):
        selection = self.olympiad_list.curselection()
        if not selection: return
        self.selected_olympiad_id = self.olympiad_list.get(selection[0])
        self.update_results_table()
        for tab in self.task_notebook.tabs(): self.task_notebook.forget(tab)

    def update_results_table(self):
        # Очистка
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        self.results_tree['columns'] = () # Сброс колонок

        conn = self.db_connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT participant_uuid, nickname, total_score, task_scores 
            FROM olympiad_results WHERE olympiad_id = ?
        """, (self.selected_olympiad_id,))
        results = cursor.fetchall()
        conn.close()

        if not results: return

        # 1. Анализируем первую запись, чтобы понять структуру (ICPC или простая)
        try:
            first_scores = json.loads(results[0][3])
        except:
            return # Битая JSON строка

        # Собираем ID задач
        task_ids = sorted(first_scores.keys(), key=lambda x: int(x) if x.isdigit() else x)
        
        # Определяем, есть ли штрафы (ICPC)
        is_icpc = False
        # Проверяем первый попавшийся результат задачи
        first_val = list(first_scores.values())[0]
        if isinstance(first_val, dict) and 'penalty' in first_val:
            is_icpc = True

        # 2. Настраиваем колонки
        cols = ['nickname']
        if is_icpc:
            cols.extend(['solved', 'penalty'])
        else:
            cols.append('total_score')
        
        cols.extend(task_ids)
        self.results_tree['columns'] = cols

        # Заголовки
        self.results_tree.heading('nickname', text='Никнейм')
        self.results_tree.column('nickname', width=150)
        
        if is_icpc:
            self.results_tree.heading('solved', text='Решено')
            self.results_tree.column('solved', width=60, anchor='center')
            self.results_tree.heading('penalty', text='Штраф')
            self.results_tree.column('penalty', width=60, anchor='center')
        else:
            self.results_tree.heading('total_score', text='Баллы')
            self.results_tree.column('total_score', width=80, anchor='center')

        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        for i, tid in enumerate(task_ids):
            self.results_tree.heading(tid, text=f'{letters[i]}')
            self.results_tree.column(tid, width=60, anchor='center')

        # 3. Обрабатываем и добавляем данные
        parsed_rows = []
        for row in results:
            uuid, nickname, db_total, scores_json = row
            scores = json.loads(scores_json)
            
            row_data = [nickname]
            
            total_score = 0
            total_penalty = 0
            
            task_values = []
            
            for tid in task_ids:
                val = scores.get(tid)
                if isinstance(val, dict):
                    # ICPC или новая структура
                    s = val.get('score', 0)
                    p = val.get('penalty', 0)
                    passed = val.get('passed', False)
                    attempts = val.get('attempts', 0)
                    
                    if is_icpc:
                        if passed:
                            total_score += 1 # Считаем решенные задачи
                            total_penalty += p
                            # Формат ячейки: "+ (штраф)" или "+2 (штраф)"
                            att_str = f"+{attempts}" if attempts > 0 else "+"
                            task_values.append(f"{att_str} ({p})")
                        else:
                            # Не решено
                            task_values.append(f"-{attempts}" if attempts > 0 else ".")
                    else:
                        # Обычные баллы
                        total_score += s
                        task_values.append(str(s))
                else:
                    # Старый формат (просто число)
                    total_score += int(val)
                    task_values.append(str(val))

            if is_icpc:
                row_data.extend([total_score, total_penalty])
            else:
                row_data.append(total_score)
                
            row_data.extend(task_values)
            
            # Сохраняем для сортировки
            sort_key = (-total_score, total_penalty) if is_icpc else (-total_score,)
            parsed_rows.append((sort_key, row_data, uuid))

        # Сортируем и вставляем
        parsed_rows.sort(key=lambda x: x[0])
        
        for _, r_data, uuid in parsed_rows:
            self.results_tree.insert('', tk.END, values=r_data, iid=uuid)

    def on_participant_select(self, event):
        selection = self.results_tree.selection()
        if not selection: return
        participant_uuid = selection[0]
        
        for tab in self.task_notebook.tabs(): self.task_notebook.forget(tab)

        conn = self.db_connect()
        cursor = conn.cursor()
        cursor.execute("SELECT task_submissions FROM olympiad_submissions WHERE olympiad_id = ? AND participant_uuid = ?",
                       (self.selected_olympiad_id, participant_uuid))
        res = cursor.fetchone()
        conn.close()

        if not res: return
        submissions = json.loads(res[0])
        
        task_ids = sorted(submissions.keys(), key=lambda x: int(x) if x.isdigit() else x)
        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

        for i, tid in enumerate(task_ids):
            code = submissions[tid]
            if not code: continue
            
            frame = ttk.Frame(self.task_notebook)
            self.task_notebook.add(frame, text=f"Задача {letters[i]}")
            
            txt = tk.Text(frame, font=("Consolas", 10))
            txt.pack(fill=tk.BOTH, expand=True)
            txt.insert(tk.END, code)
            txt.config(state=tk.DISABLED)

    def export_to_excel(self):
        try:
            conn = self.db_connect()
            df = pd.read_sql_query("SELECT * FROM olympiad_results", conn)
            conn.close()
            
            if df.empty: return

            filepath = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")])
            if not filepath: return

            with pd.ExcelWriter(filepath) as writer:
                for o_id, group in df.groupby('olympiad_id'):
                    export_data = []
                    
                    for _, row in group.iterrows():
                        scores = json.loads(row['task_scores'])
                        # Пытаемся понять ICPC ли это
                        is_icpc = False
                        if scores and isinstance(list(scores.values())[0], dict) and 'penalty' in list(scores.values())[0]:
                            is_icpc = True

                        entry = {'Никнейм': row['nickname']}
                        
                        total_s = 0
                        total_p = 0
                        
                        # Сортируем задачи
                        tids = sorted(scores.keys(), key=lambda x: int(x) if x.isdigit() else x)
                        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

                        for i, tid in enumerate(tids):
                            val = scores[tid]
                            lbl = f"Задача {letters[i]}"
                            
                            if isinstance(val, dict):
                                if is_icpc:
                                    # Для ICPC делаем красивую строку в ячейке
                                    if val.get('passed'):
                                        att = val.get('attempts', 0)
                                        pen = val.get('penalty', 0)
                                        total_s += 1
                                        total_p += pen
                                        # Формат: "+(attempts) [pen]"
                                        entry[lbl] = f"+{att if att>0 else ''} [{pen}]"
                                    else:
                                        att = val.get('attempts', 0)
                                        entry[lbl] = f"-{att}" if att > 0 else ""
                                else:
                                    # Обычные баллы
                                    s = val.get('score', 0)
                                    total_s += s
                                    entry[lbl] = s
                            else:
                                # Старый формат
                                total_s += int(val)
                                entry[lbl] = val
                        
                        if is_icpc:
                            entry['Решено задач'] = total_s
                            entry['Общий штраф'] = total_p
                        else:
                            entry['Сумма баллов'] = total_s
                            
                        export_data.append(entry)
                    
                    # Сортируем DataFrame перед записью
                    res_df = pd.DataFrame(export_data)
                    if 'Решено задач' in res_df.columns:
                         res_df = res_df.sort_values(by=['Решено задач', 'Общий штраф'], ascending=[False, True])
                    elif 'Сумма баллов' in res_df.columns:
                         res_df = res_df.sort_values(by='Сумма баллов', ascending=False)

                    # Имя листа (удаляем спецсимволы)
                    safe_name = re.sub(r'[\\/*?:"<>|]', "", str(o_id))[:30]
                    res_df.to_excel(writer, sheet_name=safe_name, index=False)
            
            messagebox.showinfo("Успех", "Экспорт завершен!")
            
        except Exception as e:
            messagebox.showerror("Ошибка", str(e))


if __name__ == "__main__":
    try:
        app = ResultsViewer()
        app.mainloop()
    except Exception as e:
        import traceback
        # Если ошибка случилась, показываем её в окне
        root = tk.Tk()
        root.withdraw() # Скрываем основное окно
        messagebox.showerror("Критическая ошибка", f"Программа упала:\n\n{traceback.format_exc()}")
        root.destroy()