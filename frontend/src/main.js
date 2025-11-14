import { createApp } from 'vue'
import App from './App.vue'
import axios from 'axios'
import './style.css'

// Configure axios defaults
axios.defaults.baseURL = window.location.origin

createApp(App).mount('#app')
