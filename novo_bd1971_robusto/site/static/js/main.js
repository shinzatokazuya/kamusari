/**
 * JavaScript Principal - Brasileirão Stats
 *
 * Este arquivo contém todas as funções de interatividade do site,
 * incluindo navegação, busca dinâmica e criação de gráficos.
 */

// ==================== INICIALIZAÇÃO ====================

document.addEventListener('DOMContentLoaded', function() {
    console.log('Site carregado! Inicializando funcionalidades...');

    // Inicializar funcionalidades
    inicializarBuscaDinamica();
    inicializarAnimacoes();
    inicializarTooltips();
});

// ==================== BUSCA DINÂMICA ====================

/**
 * Adiciona busca com sugestões em tempo real conforme usuário digita.
 * Isso melhora muito a experiência do usuário!
 */
function inicializarBuscaDinamica() {
    const inputBusca = document.querySelector('.search-input');

    if (!inputBusca) return;

    // Variável para armazenar o timeout (evita fazer muitas requisições)
    let timeoutBusca;

    inputBusca.addEventListener('input', function(e) {
        const query = e.target.value.trim();

        // Limpar timeout anterior
        clearTimeout(timeoutBusca);

        // Se query tem menos de 2 caracteres, não buscar
        if (query.length < 2) {
            esconderSugestoes();
            return;
        }

        // Aguardar 300ms antes de buscar (debounce)
        timeoutBusca = setTimeout(() => {
            buscarSugestoes(query);
        }, 300);
    });

    // Fechar sugestões ao clicar fora
    document.addEventListener('click', function(e) {
        if (!e.target.closest('.nav-search')) {
            esconderSugestoes();
        }
    });
}

/**
 * Busca sugestões no servidor via API
 */
async function buscarSugestoes(query) {
    try {
        const response = await fetch(`/api/buscar_sugestoes?q=${encodeURIComponent(query)}`);

        if (!response.ok) {
            console.error('Erro ao buscar sugestões');
            return;
        }

        const sugestoes = await response.json();
        mostrarSugestoes(sugestoes);

    } catch (error) {
        console.error('Erro na busca:', error);
    }
}

/**
 * Exibe as sugestões em uma lista dropdown
 */
function mostrarSugestoes(sugestoes) {
    // Verificar se já existe container de sugestões
    let container = document.querySelector('.sugestoes-container');

    if (!container) {
        container = document.createElement('div');
        container.className = 'sugestoes-container';
        document.querySelector('.nav-search').appendChild(container);
    }

    // Limpar sugestões anteriores
    container.innerHTML = '';

    // Se não há sugestões, esconder
    if (sugestoes.length === 0) {
        container.innerHTML = '<div class="sugestao-item sem-resultados">Nenhum resultado encontrado</div>';
        return;
    }

    // Adicionar cada sugestão
    sugestoes.forEach(sug => {
        const item = document.createElement('a');
        item.className = 'sugestao-item';
        item.href = sug.url;

        item.innerHTML = `
            <div class="sugestao-tipo">${sug.tipo}</div>
            <div class="sugestao-nome">${sug.nome}</div>
            ${sug.info ? `<div class="sugestao-info">${sug.info}</div>` : ''}
        `;

        container.appendChild(item);
    });

    container.style.display = 'block';
}

function esconderSugestoes() {
    const container = document.querySelector('.sugestoes-container');
    if (container) {
        container.style.display = 'none';
    }
}

// ==================== ANIMAÇÕES ====================

/**
 * Adiciona animações suaves ao rolar a página (scroll animations).
 * Elementos aparecem gradualmente conforme entram na tela.
 */
function inicializarAnimacoes() {
    // Criar Intersection Observer para detectar elementos na viewport
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('animado');
            }
        });
    }, {
        threshold: 0.1 // Ativa quando 10% do elemento está visível
    });

    // Observar todos os cards de seção
    const cards = document.querySelectorAll('.section-card');
    cards.forEach(card => {
        card.classList.add('animavel');
        observer.observe(card);
    });
}

// ==================== GRÁFICOS ====================

/**
 * Cria gráfico de evolução de pontos de um clube ao longo dos anos.
 * Usa Chart.js para criar visualizações bonitas e interativas.
 *
 * @param {string} canvasId - ID do elemento canvas onde desenhar
 * @param {string} nomeClube - Nome do clube para buscar dados
 */
async function criarGraficoEvolucao(canvasId, nomeClube) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    try {
        // Buscar dados da API
        const response = await fetch(`/api/evolucao_clube/${encodeURIComponent(nomeClube)}`);
        const dados = await response.json();

        // Preparar dados para o gráfico
        const anos = dados.map(d => d.ano);
        const pontos = dados.map(d => d.total_pontos);

        // Criar gráfico
        new Chart(canvas, {
            type: 'line',
            data: {
                labels: anos,
                datasets: [{
                    label: 'Pontos por Ano',
                    data: pontos,
                    borderColor: '#1e88e5',
                    backgroundColor: 'rgba(30, 136, 229, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4 // Suaviza a linha
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            title: function(context) {
                                return 'Ano ' + context[0].label;
                            },
                            label: function(context) {
                                return context.parsed.y + ' pontos';
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Pontos'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Ano'
                        }
                    }
                }
            }
        });

    } catch (error) {
        console.error('Erro ao criar gráfico:', error);
    }
}

/**
 * Cria gráfico de pizza mostrando distribuição de resultados (V/E/D).
 *
 * @param {string} canvasId - ID do canvas
 * @param {object} dados - Objeto com vitorias, empates e derrotas
 */
function criarGraficoPizza(canvasId, dados) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    new Chart(canvas, {
        type: 'doughnut',
        data: {
            labels: ['Vitórias', 'Empates', 'Derrotas'],
            datasets: [{
                data: [dados.vitorias, dados.empates, dados.derrotas],
                backgroundColor: [
                    '#43a047', // Verde para vitórias
                    '#ffa726', // Laranja para empates
                    '#f44336'  // Vermelho para derrotas
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed;
                            const total = dados.vitorias + dados.empates + dados.derrotas;
                            const percentage = ((value / total) * 100).toFixed(1);
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
}

/**
 * Cria gráfico de barras comparando estatísticas de múltiplos clubes.
 *
 * @param {string} canvasId - ID do canvas
 * @param {Array} clubes - Array com nomes dos clubes
 */
async function criarGraficoComparacao(canvasId, clubes) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    try {
        // Montar query string com array de clubes
        const params = new URLSearchParams();
        clubes.forEach(clube => params.append('clubes[]', clube));

        const response = await fetch(`/api/comparacao_clubes?${params.toString()}`);
        const dados = await response.json();

        // Preparar dados
        const labels = dados.map(d => d.clube);
        const pontos = dados.map(d => d.pontos);
        const jogos = dados.map(d => d.jogos);

        new Chart(canvas, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Total de Pontos',
                        data: pontos,
                        backgroundColor: '#1e88e5',
                        borderWidth: 0
                    },
                    {
                        label: 'Total de Jogos',
                        data: jogos,
                        backgroundColor: '#43a047',
                        borderWidth: 0
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

    } catch (error) {
        console.error('Erro ao criar gráfico de comparação:', error);
    }
}

// ==================== TOOLTIPS ====================

/**
 * Adiciona tooltips informativos em elementos que precisam de explicação adicional.
 * Por exemplo, pode explicar o que significa "SG" (Saldo de Gols) na tabela.
 */
function inicializarTooltips() {
    const elementosComTooltip = document.querySelectorAll('[data-tooltip]');

    elementosComTooltip.forEach(elemento => {
        const tooltip = document.createElement('div');
        tooltip.className = 'tooltip';
        tooltip.textContent = elemento.getAttribute('data-tooltip');

        elemento.addEventListener('mouseenter', function() {
            document.body.appendChild(tooltip);

            // Posicionar tooltip
            const rect = elemento.getBoundingClientRect();
            tooltip.style.left = rect.left + (rect.width / 2) + 'px';
            tooltip.style.top = rect.top - tooltip.offsetHeight - 10 + 'px';

            tooltip.classList.add('visivel');
        });

        elemento.addEventListener('mouseleave', function() {
            tooltip.classList.remove('visivel');
            setTimeout(() => {
                if (tooltip.parentNode) {
                    document.body.removeChild(tooltip);
                }
            }, 200);
        });
    });
}

// ==================== FUNÇÕES UTILITÁRIAS ====================

/**
 * Formata número com separadores de milhares.
 * Exemplo: 1000000 -> "1.000.000"
 */
function formatarNumero(numero) {
    return numero.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

/**
 * Formata data no padrão brasileiro.
 * Exemplo: "2024-03-15" -> "15/03/2024"
 */
function formatarData(dataISO) {
    if (!dataISO) return '';

    const [ano, mes, dia] = dataISO.split('-');
    return `${dia}/${mes}/${ano}`;
}

/**
 * Detecta cor do time baseado no resultado do jogo.
 * Útil para destacar vitórias, empates e derrotas visualmente.
 */
function corResultado(golsPro, golsContra) {
    if (golsPro > golsContra) return 'vitoria';
    if (golsPro < golsContra) return 'derrota';
    return 'empate';
}

// ==================== EXPORTAR FUNÇÕES GLOBAIS ====================

// Disponibilizar funções que podem ser chamadas de outros scripts ou inline
window.BrasileiraoStats = {
    criarGraficoEvolucao,
    criarGraficoPizza,
    criarGraficoComparacao,
    formatarNumero,
    formatarData,
    corResultado
};
