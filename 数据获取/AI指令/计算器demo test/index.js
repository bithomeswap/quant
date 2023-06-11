const operatorRegex = /[-+*/^√]/g;

const display = document.getElementById("result");
const keypad = document.querySelector(".keypad");

let currentExpression = "";
let lastOperatorIndex = -1;

const updateDisplay = () => {
    display.value = currentExpression;
};

const calculate = () => {
    const expression = currentExpression.replace(/^\s+|\s+$/g, "");
    const operators = expression.match(operatorRegex);
    let result = 0;

    if (!operators) return;

    for (let i = 0; i < operators.length; i++) {
        const operator = operators[i];
        const operatorIndex = expression.indexOf(operator, lastOperatorIndex + 1);
        const operand1 = parseFloat(expression.slice(lastOperatorIndex + 1, operatorIndex).replace(/\s+/g, ""));
        const operand2 = parseFloat(expression.slice(operatorIndex + 1, expression.indexOf(operators[i + 1], operatorIndex + 1)).replace(/\s+/g, ""));

        if (operator === "+") {
            result += operand2;
        } else if (operator === "-") {
            result -= operand2;
        } else if (operator === "*") {
            result *= operand2;
        } else if (operator === "/") {
            result /= operand2;
        } else if (operator === "^") {
            result = Math.pow(operand1, operand2);
        } else if (operator === "√") {
            result = Math.sqrt(operand2);
        }

        lastOperatorIndex = operatorIndex;
    }

    currentExpression = result.toString();
    updateDisplay();
};

const clearAll = () => {
    currentExpression = "";
    lastOperatorIndex = -1;
    updateDisplay();
};

const backspace = () => {
    currentExpression = currentExpression.slice(0, -1);
    lastOperatorIndex = currentExpression.search(operatorRegex) || -1;
    updateDisplay();
};

const addDecimal = () => {
    if (!currentExpression.match(/[-+*/^√]?\d*\.\d*([-+*/^√](?=\d)|$)/g)) {
        currentExpression += ".";
        updateDisplay();
    }
};

keypad.addEventListener("click", (event) => {
    const { target } = event;

    if (target.id === "clear") {
        clearAll();
    } else if (target.id === "backspace") {
        backspace();
    } else if (target.id === "equals") {
        calculate();
    } else if (target.classList.contains("operator")) {
        if (target.id === "squareRoot") {
            currentExpression += "√";
            updateDisplay();
        } else if (!currentExpression.match(/[√][-+*/^√]/g)) {
            currentExpression += target.textContent;
            lastOperatorIndex = currentExpression.lastIndexOf(target.textContent);
            updateDisplay();
        }
    } else {
        currentExpression += target.textContent;
        updateDisplay();
    }
});

document.addEventListener("keydown", (event) => {
    const { key } = event;

    if (/^[0-9]$/.test(key)) {
        currentExpression += key;
        updateDisplay();
    } else if (key === ".") {
        addDecimal();
    } else if (/^[\/*\-+^]$/.test(key)) {
        if (/^[\/*^]$/.test(key)) {
            lastOperatorIndex = currentExpression.lastIndexOf(key);
        } else if (/^-/.test(key) && !currentExpression.trim()) {
            currentExpression += key;
            updateDisplay();
            return;
        } else {
            lastOperatorIndex = currentExpression.search(operatorRegex);
        }

        if (lastOperatorIndex === -1) {
            currentExpression += key;
            updateDisplay();
        } else if (operatorRegex.test(currentExpression[lastOperatorIndex + 1])) {
            currentExpression = `${currentExpression.slice(0, lastOperatorIndex + 1)} ${key} ${currentExpression.slice(lastOperatorIndex + 2)}`;
            lastOperatorIndex++;
            updateDisplay();
        }
    } else if (key === "Enter") {
        calculate();
    } else if (key === "Backspace") {
        backspace();
    } else if (key === "Delete") {
        clearAll();
    }
});

display.addEventListener("input", (event) => {
    currentExpression = event.target.value;
    lastOperatorIndex = currentExpression.search(operatorRegex) || -1;
});
