const app = new Vue({
    el: '#app',
    data: {
        inputValue: '',
    },
    methods: {
        input(value) {
            this.inputValue += value;
        },
        backspace() {
            this.inputValue = this.inputValue.slice(0, -1);
        },
        clear() {
            this.inputValue = '';
        },
        calculate() {
            try {
                const value = eval(this.inputValue);
                this.inputValue = value.toFixed(10);
            } catch (e) {
                this.inputValue = '计算错误';
            }
        },
        sqrt() {
            try {
                const value = Math.sqrt(eval(this.inputValue));
                this.inputValue = value.toFixed(10);
            } catch (e) {
                this.inputValue = '计算错误';
            }
        },
    },
});
