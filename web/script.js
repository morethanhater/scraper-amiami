document.addEventListener('DOMContentLoaded', () => {
    const DEFAULT_VALUE = 'N/A';
    const YEN_TO_EURO_MULTIPLIER = 0.006106; // As of 2025-05-09
    const FOLDER_PATH = 'data/';
    const DATA_FILES_PATH = '_data_files.txt';
    const BATCH_SIZE = 50;
    const INPUT_DEBOUNCE_MS = 500;
    const ITEM_CONDITION_ORDER = ['J', 'C', 'B', 'B+', 'A-', 'A', ''];
    const NEW_ITEM_FLAGS = [
        'is_preowned',
    //   'is_preorder',
        'is_backorder',
    //    'has_store_bonus',
    //    'is_amiami_limited',
        'is_age_limited',
    //    'has_preorder_bonus',
        'is_preowned_sale',
    ];

    const elements = {
        searchInput: document.getElementById('searchInput'),
        minPriceInput: document.getElementById('minPrice'),
        maxPriceInput: document.getElementById('maxPrice'),
        minDiscountInput: document.getElementById('minDiscount'),
        maxDiscountInput: document.getElementById('maxDiscount'),
        itemsCount: document.getElementById('itemsCount'),
        itemsTableBody: document.getElementById('itemsTableBody'),
        sortableHeaders: Array.from(document.querySelectorAll('.sortable')),
        triStateCheckboxes: Array.from(document.querySelectorAll('input[data-tristate="true"]')),
        imageModal: document.getElementById('imageModal'),
        imageModalBackdrop: document.getElementById('imageModalBackdrop'),
        imageModalClose: document.getElementById('imageModalClose'),
        imageModalPreview: document.getElementById('imageModalPreview'),
    };

    const state = {
        currentIndex: 0,
        jsonData: [],
        filteredData: [],
        currentSort: { column: null, direction: null },
    };

    let searchInputTimeoutId;
    let minPriceTimeoutId;
    let maxPriceTimeoutId;
    let minDiscountTimeoutId;
    let maxDiscountTimeoutId;

    const renderText = (content) => (content instanceof Node ? content : document.createTextNode(content));

    const createCell = (content) => {
        const cell = document.createElement('td');
        cell.appendChild(renderText(content));
        return cell;
    };

    const createLink = (href, text) => {
        const link = document.createElement('a');
        link.href = href;
        link.target = '_blank';
        link.textContent = text || DEFAULT_VALUE;
        return link;
    };

    const createSplitCell = (topContent, bottomContent) => {
        const wrapper = document.createElement('span');
        wrapper.className = 'split-cell';

        const top = document.createElement('span');
        top.className = 'top';
        top.appendChild(renderText(topContent));

        const bottom = document.createElement('span');
        bottom.className = 'bottom';
        bottom.appendChild(renderText(bottomContent));

        wrapper.appendChild(top);
        wrapper.appendChild(bottom);
        return createCell(wrapper);
    };

    const formatEuroPrice = (yenPrice) => `${(yenPrice * YEN_TO_EURO_MULTIPLIER).toFixed(2)} €`;

    const formatReleaseDate = (releaseDate) => (
        releaseDate
            ? new Date(releaseDate).toLocaleDateString('en-GB', {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
            })
            : DEFAULT_VALUE
    );

    const getDiscountPercent = (item) => {
        if (!item.full_price || !item.price || item.full_price <= 0 || item.price >= item.full_price) {
            return 0;
        }
        return ((item.full_price - item.price) / item.full_price) * 100;
    };

    const isStrictlyNewItem = (item) => NEW_ITEM_FLAGS.every(flag => item[flag] !== true);

    const openImageModal = (src, alt) => {
        elements.imageModalPreview.src = src;
        elements.imageModalPreview.alt = alt || 'Full-size preview';
        elements.imageModal.classList.remove('hidden');
        elements.imageModal.setAttribute('aria-hidden', 'false');
    };

    const closeImageModal = () => {
        elements.imageModal.classList.add('hidden');
        elements.imageModal.setAttribute('aria-hidden', 'true');
        elements.imageModalPreview.src = '';
    };

    const getSelectedFilterValues = (name) => (
        Array.from(document.querySelectorAll(`input[name="${name}"]:checked`)).map((element) => element.value)
    );

    const getTriStateValues = (name, targetState) => (
        Array.from(document.querySelectorAll(`input[name="${name}"][data-tristate="true"]`))
            .filter((element) => element.dataset.state === targetState)
            .map((element) => element.value)
    );

    const getTriStateFilters = (name) => ({
        include: getTriStateValues(name, 'include'),
        exclude: getTriStateValues(name, 'exclude'),
    });

    const parsePriceInput = (value, fallback) => parseFloat(value.replace(',', '.')) || fallback;

    const assignOriginalIndexes = (items, startIndex = 0) => {
        items.forEach((item, index) => {
            item.__originalIndex = startIndex + index;
        });
        return items;
    };

    const matchesSearchQuery = (item, query) => (
        item.name.toLowerCase().includes(query)
        || item.gcode.toLowerCase().includes(query)
        || item.scode.toLowerCase().includes(query)
        || item.jancode?.toLowerCase().includes(query)
        || item.tags.some(tag => tag.toLowerCase().includes(query))
        || item.maker_name.toLowerCase().includes(query)
        || item.modeler_name.toLowerCase().includes(query)
        || item.description.toLowerCase().includes(query)
    );

    const matchesConditionFilters = (item, itemConditionFilters, boxConditionFilters) => {
        const matchesIncludedItemCondition = itemConditionFilters.include.length === 0
            || itemConditionFilters.include.includes(item.item_condition);
        const matchesExcludedItemCondition = !itemConditionFilters.exclude.includes(item.item_condition);
        const matchesIncludedBoxCondition = boxConditionFilters.include.length === 0
            || boxConditionFilters.include.includes(item.box_condition);
        const matchesExcludedBoxCondition = !boxConditionFilters.exclude.includes(item.box_condition);

        const matchesItemCondition = matchesIncludedItemCondition && matchesExcludedItemCondition;
        const matchesBoxCondition = matchesIncludedBoxCondition && matchesExcludedBoxCondition;
        return matchesItemCondition && matchesBoxCondition;
    };

    const matchesDetailFilters = (item, itemBoolDetailFilters) => {
        const requireStrictlyNew = itemBoolDetailFilters.include.includes('is_new');
        const excludeStrictlyNew = itemBoolDetailFilters.exclude.includes('is_new');
        const positiveIncludedItemBoolDetails = itemBoolDetailFilters.include.filter(filter => filter !== 'is_new');
        const positiveExcludedItemBoolDetails = itemBoolDetailFilters.exclude.filter(filter => filter !== 'is_new');
        const matchesNew = (!requireStrictlyNew || isStrictlyNewItem(item))
            && (!excludeStrictlyNew || !isStrictlyNewItem(item));
        const matchesIncludedBool = positiveIncludedItemBoolDetails.length === 0
            || positiveIncludedItemBoolDetails.every(filter => item[filter] === true);
        const matchesExcludedBool = positiveExcludedItemBoolDetails.every(filter => item[filter] !== true);

        return matchesNew && matchesIncludedBool && matchesExcludedBool;
    };

    const matchesPriceFilter = (item, minPrice, maxPrice) => {
        const euroPrice = Number((item.price * YEN_TO_EURO_MULTIPLIER).toFixed(2));
        return euroPrice >= minPrice && euroPrice <= maxPrice;
    };

    const matchesDiscountFilter = (item, minDiscount, maxDiscount) => {
        const discountPercent = getDiscountPercent(item);
        return discountPercent >= minDiscount && discountPercent <= maxDiscount;
    };

    const buildImageCell = (item) => {
        const image = document.createElement('img');
        image.src = item.image_url;
        image.alt = item.name;
        image.loading = 'lazy';
        image.addEventListener('click', () => {
            openImageModal(item.image_url, item.name);
        });
        return createCell(image);
    };

    const buildJanCodeCell = (item) => (
        item.jancode
            ? createCell(createLink(`https://myfigurecollection.net/?keywords=${item.jancode}&_tb=item`, item.jancode))
            : createCell(DEFAULT_VALUE)
    );

    const buildConditionCell = (item) => createSplitCell(
        item.item_condition ? `ITEM: ${item.item_condition}` : 'New',
        item.box_condition ? `BOX: ${item.box_condition}` : 'New',
    );

    const buildRow = (item, rowIndex, totalItems) => {
        const row = document.createElement('tr');
        const originalPrice = item.full_price || item.price;

        row.appendChild(createCell(`${rowIndex + 1} / ${totalItems}`));
        row.appendChild(buildImageCell(item));
        row.appendChild(createCell(item.name));
        row.appendChild(createSplitCell(
            createLink(item.gcode_url, item.gcode),
            createLink(item.scode_url, item.scode || DEFAULT_VALUE),
        ));
        row.appendChild(createSplitCell(`¥${item.price}`, formatEuroPrice(item.price)));
        row.appendChild(createSplitCell(`¥${originalPrice}`, formatEuroPrice(originalPrice)));
        row.appendChild(createCell(`${getDiscountPercent(item).toFixed(1)}%`));
        row.appendChild(createCell(item.sale_status || DEFAULT_VALUE));
        row.appendChild(createCell(formatReleaseDate(item.release_date)));
        row.appendChild(buildJanCodeCell(item));
        row.appendChild(buildConditionCell(item));

        return row;
    };

    const updateDisplayedCount = () => {
        elements.itemsCount.textContent = state.filteredData.length;
    };

    const clearTable = () => {
        elements.itemsTableBody.innerHTML = '';
    };

    const displayData = () => {
        const endIndex = state.currentIndex + BATCH_SIZE;
        const currentBatch = state.filteredData.slice(state.currentIndex, endIndex);

        currentBatch.forEach((item, index) => {
            const rowIndex = state.currentIndex + index;
            elements.itemsTableBody.appendChild(buildRow(item, rowIndex, state.filteredData.length));
        });

        state.currentIndex = endIndex;
        updateDisplayedCount();
    };

    const filterData = () => {
        const query = elements.searchInput.value.toLowerCase();
        const itemConditionFilters = getTriStateFilters('item_condition');
        const boxConditionFilters = getTriStateFilters('box_condition');
        const itemBoolDetailFilters = getTriStateFilters('item_bool_details');
        const minPrice = parsePriceInput(elements.minPriceInput.value, 0.0);
        const maxPrice = parsePriceInput(elements.maxPriceInput.value, Infinity);
        const minDiscount = parsePriceInput(elements.minDiscountInput.value, 0.0);
        const maxDiscount = parsePriceInput(elements.maxDiscountInput.value, 100.0);

        return state.jsonData.filter(item => (
            matchesSearchQuery(item, query)
            && matchesConditionFilters(item, itemConditionFilters, boxConditionFilters)
            && matchesDetailFilters(item, itemBoolDetailFilters)
            && matchesPriceFilter(item, minPrice, maxPrice)
            && matchesDiscountFilter(item, minDiscount, maxDiscount)
        ));
    };

    const getComparableValue = (item, column) => {
        if (!column) {
            return item.__originalIndex ?? 0;
        }
        if (column === 'release_date') {
            return new Date(item.release_date);
        }
        if (column === 'discount_percent') {
            return getDiscountPercent(item);
        }
        if (column === 'item_condition') {
            const index = ITEM_CONDITION_ORDER.indexOf(item.item_condition);
            return index === -1 ? ITEM_CONDITION_ORDER.length : index;
        }
        return item[column];
    };

    const sortData = (column, direction) => {
        const effectiveColumn = direction ? column : null;

        state.filteredData.sort((a, b) => {
            const valueA = getComparableValue(a, effectiveColumn);
            const valueB = getComparableValue(b, effectiveColumn);

            if (!direction) {
                return valueA > valueB ? 1 : valueA < valueB ? -1 : 0;
            }

            if (direction === 'asc') {
                return valueA > valueB ? 1 : valueA < valueB ? -1 : 0;
            }
            return valueA < valueB ? 1 : valueA > valueB ? -1 : 0;
        });

        clearTable();
        state.currentIndex = 0;
        displayData();
    };

    const updateSortHeaders = (direction, activeHeader) => {
        elements.sortableHeaders.forEach(header => header.classList.remove('sorted-asc', 'sorted-desc'));
        if (activeHeader && direction) {
            activeHeader.classList.add(direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
        }
    };

    const getNextSortDirection = (column) => {
        if (state.currentSort.column !== column || state.currentSort.direction === null) {
            return 'desc';
        }
        if (state.currentSort.direction === 'desc') {
            return 'asc';
        }
        return null;
    };

    const setupSort = (column, headerElement) => {
        const direction = getNextSortDirection(column);
        state.currentSort = {
            column: direction ? column : null,
            direction,
        };
        sortData(column, direction);
        updateSortHeaders(direction, direction ? headerElement : null);
    };

    const refreshView = () => {
        state.filteredData = filterData();
        state.currentIndex = 0;
        sortData(state.currentSort.column, state.currentSort.direction);
    };

    const scheduleRefresh = (timeoutRefSetter) => {
        clearTimeout(timeoutRefSetter.current);
        timeoutRefSetter.current = setTimeout(() => {
            refreshView();
        }, INPUT_DEBOUNCE_MS);
    };

    const applyTriStateAppearance = (checkbox) => {
        const label = checkbox.closest('label');
        const stateValue = checkbox.dataset.state || 'ignore';

        checkbox.checked = stateValue === 'include';
        checkbox.indeterminate = stateValue === 'exclude';
        checkbox.setAttribute(
            'aria-checked',
            stateValue === 'exclude' ? 'mixed' : (stateValue === 'include' ? 'true' : 'false'),
        );

        if (label) {
            label.classList.remove('tri-state-ignore', 'tri-state-include', 'tri-state-exclude');
            label.classList.add(`tri-state-${stateValue}`);
        }
    };

    const advanceTriState = (checkbox) => {
        const currentState = checkbox.dataset.state || 'ignore';
        const nextState = currentState === 'ignore'
            ? 'include'
            : (currentState === 'include' ? 'exclude' : 'ignore');

        checkbox.dataset.state = nextState;
        applyTriStateAppearance(checkbox);
    };

    const initializeTriStateCheckboxes = () => {
        elements.triStateCheckboxes.forEach((checkbox) => {
            checkbox.dataset.state = 'ignore';
            applyTriStateAppearance(checkbox);
        });
    };

    const registerInputListeners = () => {
        elements.searchInput.addEventListener('input', () => {
            scheduleRefresh({
                get current() { return searchInputTimeoutId; },
                set current(value) { searchInputTimeoutId = value; },
            });
        });

        elements.triStateCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('click', (event) => {
                event.preventDefault();
                advanceTriState(checkbox);
                refreshView();
            });
        });

        elements.minPriceInput.addEventListener('input', () => {
            scheduleRefresh({
                get current() { return minPriceTimeoutId; },
                set current(value) { minPriceTimeoutId = value; },
            });
        });

        elements.maxPriceInput.addEventListener('input', () => {
            scheduleRefresh({
                get current() { return maxPriceTimeoutId; },
                set current(value) { maxPriceTimeoutId = value; },
            });
        });

        elements.minDiscountInput.addEventListener('input', () => {
            scheduleRefresh({
                get current() { return minDiscountTimeoutId; },
                set current(value) { minDiscountTimeoutId = value; },
            });
        });

        elements.maxDiscountInput.addEventListener('input', () => {
            scheduleRefresh({
                get current() { return maxDiscountTimeoutId; },
                set current(value) { maxDiscountTimeoutId = value; },
            });
        });
    };

    const registerScrollListener = () => {
        window.addEventListener('scroll', () => {
            const nearBottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 200;
            if (nearBottom && state.currentIndex < state.filteredData.length) {
                displayData();
            }
        });
    };

    const registerModalListeners = () => {
        elements.imageModalClose.addEventListener('click', closeImageModal);
        elements.imageModalBackdrop.addEventListener('click', (event) => {
            if (event.target === elements.imageModalBackdrop) {
                closeImageModal();
            }
        });
        document.addEventListener('keydown', (event) => {
            if (event.key === 'Escape' && !elements.imageModal.classList.contains('hidden')) {
                closeImageModal();
            }
        });
    };

    const registerSortListeners = () => {
        elements.sortableHeaders.forEach(header => {
            header.addEventListener('click', () => {
                setupSort(header.dataset.column, header);
            });
        });
    };

    const loadEmbeddedData = () => {
        if (!Array.isArray(window.__AMIAMI_EMBEDDED_DATA__)) {
            return false;
        }

        console.log(`Using embedded dataset, ${window.__AMIAMI_EMBEDDED_DATA__.length} items found.`);
        state.jsonData = assignOriginalIndexes(window.__AMIAMI_EMBEDDED_DATA__);
        state.filteredData = state.jsonData;
        refreshView();
        return true;
    };

    const loadDataFilesList = async () => {
        const response = await fetch(FOLDER_PATH + DATA_FILES_PATH);
        if (!response.ok) {
            throw new Error('Error while loading the data file.');
        }

        const dataText = await response.text();
        return dataText
            .split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0 && !line.startsWith('#'));
    };

    const loadJsonFile = async (filename) => {
        console.log(`> Loading '${filename}'`);
        const response = await fetch(FOLDER_PATH + filename);
        if (!response.ok) {
            throw new Error('Error while loading JSON file.');
        }
        return response.json();
    };

    const loadRemoteData = async () => {
        const filenames = await loadDataFilesList();
        console.log(`Found ${filenames.length} JSON files.`);

        try {
            for (const filename of filenames) {
                const data = await loadJsonFile(filename);
                const indexedItems = assignOriginalIndexes(data.items, state.jsonData.length);
                state.jsonData = state.jsonData.concat(indexedItems);
            }
        } catch (error) {
            console.error('Error while loading JSON files:', error);
        }

        console.log(`All files loaded, ${state.jsonData.length} items found.`);
        state.filteredData = state.jsonData;
        refreshView();
    };

    const loadData = async () => {
        if (loadEmbeddedData()) {
            return;
        }
        await loadRemoteData();
    };

    initializeTriStateCheckboxes();
    registerInputListeners();
    registerScrollListener();
    registerModalListeners();
    registerSortListeners();
    loadData();
});
