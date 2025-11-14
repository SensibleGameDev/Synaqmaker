// file: static/js/main.js

document.addEventListener('DOMContentLoaded', function() {
    // --- ИНИЦИАЛИЗАЦИЯ CODEMIRROR ---
    
    const codeTextArea = document.getElementById('code');

    if (!codeTextArea) {
        return; // Выход из функции
    }
    const editor = CodeMirror.fromTextArea(codeTextArea, {
        lineNumbers: true,
        mode: "python", // Язык по умолчанию
        theme: "material-darker",
        matchBrackets: true,
        indentUnit: 4
    });

    // --- СМЕНА ЯЗЫКА ДЛЯ ПОДСВЕТКИ СИНТАКСИСА ---
    const languageSelect = document.getElementById('language');
    languageSelect.addEventListener('change', () => {
        // CodeMirror использует специальные 'mode' для языков
        const newMode = languageSelect.value === 'C++' ? 'text/x-c++src' : 'python';
        editor.setOption("mode", newMode);
    });

    // --- ОБРАБОТКА ОТПРАВКИ ФОРМЫ ---
    document.getElementById('submission-form').addEventListener('submit', function(event) {
        event.preventDefault();

        const form = event.target;
        const taskId = form.task_id.value;
        const language = form.language.value;
        const code = editor.getValue(); // <-- ИЗМЕНЕНИЕ: получаем код из редактора
        const resultsContainer = document.getElementById('results-container');
        const spinner = document.getElementById('spinner');
        const submitButton = form.querySelector('button[type="submit"]');
        
        if (!taskId || !code) {
            alert('Пожалуйста, выберите задачу и введите код.');
            return;
        }

        spinner.classList.remove('d-none');
        submitButton.disabled = true;
        resultsContainer.innerHTML = '';

        fetch('/run_code', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                task_id: taskId,
                language: language,
                code: code,
            }),
        })
        .then(response => response.json())
        .then(data => {
            spinner.classList.add('d-none');
            submitButton.disabled = false;
            
            if (data.error) {
                resultsContainer.innerHTML = `<div class="alert alert-danger">${data.error}</div>`;
                return;
            }

            const overallStatus = data.passed_count === data.total_tests ? 'success' : 'danger';
            let resultsHTML = `
                <div class="alert alert-${overallStatus}">
                    <h4 class="alert-heading">Результат: ${data.passed_count} из ${data.total_tests} тестов пройдено.</h4>
                </div>
            `;

            data.details.forEach(test => {
                
                // --- (ВОТ УЛУЧШЕННАЯ ЛОГИКА ОТОБРАЖЕНИЯ) ---
                let testResultClass = 'border-secondary';
                let testHeaderClass = 'bg-light';
                let statusIcon = `<span class="badge bg-secondary">${test.verdict}</span>`;
    
                if (test.verdict === 'Accepted') {
                    testResultClass = 'border-success';
                    testHeaderClass = 'bg-success-subtle';
                    statusIcon = `<span class="badge bg-success">${test.verdict}</span>`;
                } else if (test.verdict === 'Wrong Answer') {
                    testResultClass = 'border-warning';
                    testHeaderClass = 'bg-warning-subtle';
                    statusIcon = `<span class="badge bg-warning text-dark">${test.verdict}</span>`;
                } else if (test.verdict !== 'Accepted') {
                    // TLE, RE, CE и т.д.
                    testResultClass = 'border-danger';
                    testHeaderClass = 'bg-danger-subtle';
                    statusIcon = `<span class="badge bg-danger">${test.verdict}</span>`;
                }
                // --- (КОНЕЦ УЛУЧШЕНИЙ) ---

                /* (Это старый код, который мы заменили)
                const testResultClass = test.passed ? 'border-success' : 'border-danger';
                const testHeaderClass = test.passed ? 'bg-success-subtle' : 'bg-danger-subtle';
                const statusIcon = test.passed ? 
                    '<span class="badge bg-success">Пройден</span>' : 
                    '<span class="badge bg-danger">Ошибка</span>';
                */

                resultsHTML += `
                    <div class="card mb-3 ${testResultClass}">
                        <div class="card-header ${testHeaderClass}">
                            <strong>Тест ${test.test_num}</strong> ${statusIcon}
                        </div>
                        <div class="card-body">
                            <div class="row">
                                <div class="col-md-6">
                                    <h6>Входные данные:</h6>
                                    <pre class="bg-light p-2 rounded code-area">${test.input || '(пусто)'}</pre>
                                    <h6>Ожидаемый вывод:</h6>
                                    <pre class="bg-light p-2 rounded code-area">${test.expected}</pre>
                                </div>
                                <div class="col-md-6">
                                    <h6>Вывод программы:</h6>
                                    <pre class="bg-light p-2 rounded code-area">${test.output}</pre>
                                    ${test.error.trim() ? `
                                    <h6>Ошибка выполнения:</h6>
                                    <pre class="bg-danger-subtle text-danger p-2 rounded code-area">${test.error}</pre>
                                    ` : ''}
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            });
            resultsContainer.innerHTML = resultsHTML;
        })
        .catch(error => {
            spinner.classList.add('d-none');
            submitButton.disabled = false;
            resultsContainer.innerHTML = `<div class="alert alert-danger">Произошла ошибка сети: ${error}</div>`;
        });
    });
});