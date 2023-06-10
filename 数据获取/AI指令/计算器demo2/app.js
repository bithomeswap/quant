const screen = document.querySelector('.screen');
const calculator = document.querySelector('#calculator');
const sqrt = document.querySelector('.sqrt');
const power = document.querySelector('.power');
const equal = document.querySelector('.equal');
const clear = document.querySelector('.clear');

let expression = '';

function operate(operator) {
    if (expression && '+-*/^'.includes(expression[expression.length - 1])) {
        expression = expression.slice(0, -1);
    }
    expression += operator;
    screen.textContent = expression;
}

function appendNumber(number) {
    if (expression === '0') {
        expression = number;
    } else {
        expression += number;
    }
    screen.textContent = expression;
}

function calculate() {
    try {
        const result = eval(expression);
        screen.textContent = result;
        expression = result;
    } catch (err) {
        console.log(err);
        screen.textContent = 'Error';
        expression = '';
    }
}

function clearScreen() {
    expression = '';
    screen.textContent = expression;
}

function calculateSqrt() {
    const lastOperatorIndex = expression.search(/[-+*/^]/g);
    if (lastOperatorIndex !== -1) {
        const number = expression.slice(lastOperatorIndex + 1);
        const result = Math.sqrt(Number(number));
        expression = expression.slice(0, lastOperatorIndex + 1) + result;
        screen.textContent = expression;
    } else {
        const result = Math.sqrt(Number(expression));
        expression = result;
        screen.textContent = expression;
    }
}

function calculatePower() {
    const lastOperatorIndex = expression.search(/[-+*/^]/g);
    if (lastOperatorIndex !== -1) {
        const number = expression.slice(lastOperatorIndex + 1);
        const result = Math.pow(Number(number), 2);
        expression = expression.slice(0, lastOperatorIndex + 1) + result;
        screen.textContent = expression;
    } else {
        const result = Math.pow(Number(expression), 2);
        expression = result;
        screen.textContent = expression;
    }
}

calculator.addEventListener('click', (event) => {
    const target = event.target;
    if (target.classList.contains('number')) {
        appendNumber(target.textContent);
    } else if (target.classList.contains('operator')) {
        operate(target.textContent);
    } else if (target.classList.contains('equal')) {
        calculate();
    } else if (target.classList.contains('clear')) {
        clearScreen();
    } else if (target.classList.contains('sqrt')) {
        calculateSqrt();
    } else if (target.classList.contains('power')) {
        calculatePower();
    }
});
