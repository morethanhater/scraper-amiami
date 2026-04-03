document.addEventListener('DOMContentLoaded', () => {
    const yenToEuroMultiplier = 0.006106;   // As of 2025-05-09
    const defaultValue = 'N/A';

    const folderPath = 'data/';
    const dataFilesPath = '_data_files.txt';

    let currentIndex = 0;
    const batchSize = 50;
    const timeoutValue = 500;
    const itemConditionOrder = ['J', 'C', 'B', 'B+', 'A-', 'A', ''];

    let jsonData = [];
    let filteredData = [];

    let currentSort = { column: 'name', direction: 'asc' };

    const minPriceInput = document.getElementById('minPrice');
    const maxPriceInput = document.getElementById('maxPrice');

    // Load JSON data
    const loadData = async () => {
        if (Array.isArray(window.__AMIAMI_EMBEDDED_DATA__)) {
            console.log(`Using embedded dataset, ${window.__AMIAMI_EMBEDDED_DATA__.length} items found.`);
            jsonData = window.__AMIAMI_EMBEDDED_DATA__;
            filteredData = jsonData;
            refreshView();
            return;
        }

        // Read file listing JSON files to load
        const dataResponse = await fetch(folderPath + dataFilesPath);
        if (!dataResponse.ok) throw new Error('Error while loading the data file.');

        // Retrieve file names
        const dataText = await dataResponse.text();
        const filenames = dataText
            .split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0 && !line.startsWith('#'));
        console.log(`Found ${filenames.length} JSON files.`);

        try {
            // Load JSON files
            for (const filename of filenames) {
                console.log(`> Loading '${filename}'`);
                const response = await fetch(folderPath + filename);
                if (!response.ok) {
                    throw new Error('Error while loading JSON file.');
                }
                const data = await response.json();
                jsonData = jsonData.concat(data.items);
            }
        } catch (error) {
            console.error('Error while loading JSON files:', error);
        }

        console.log(`All files loaded, ${jsonData.length} items found.`);
        filteredData = jsonData;
        refreshView();
    }

    const getDiscountPercent = (item) => {
        if (!item.full_price || !item.price || item.full_price <= 0 || item.price >= item.full_price) {
            return 0;
        }
        return ((item.full_price - item.price) / item.full_price) * 100;
    };

    const createCell = (content) => {
        const cell = document.createElement('td');
        if (content instanceof Node) {
            cell.appendChild(content);
        } else {
            cell.textContent = content;
        }
        return cell;
    };

    const createLink = (href, text) => {
        const link = document.createElement('a');
        link.href = href;
        link.target = '_blank';
        link.textContent = text || defaultValue;
        return link;
    };

    const createSplitCell = (topContent, bottomContent) => {
        const wrapper = document.createElement('span');
        wrapper.className = 'split-cell';

        const top = document.createElement('span');
        top.className = 'top';
        if (topContent instanceof Node) {
            top.appendChild(topContent);
        } else {
            top.textContent = topContent;
        }

        const bottom = document.createElement('span');
        bottom.className = 'bottom';
        if (bottomContent instanceof Node) {
            bottom.appendChild(bottomContent);
        } else {
            bottom.textContent = bottomContent;
        }

        wrapper.appendChild(top);
        wrapper.appendChild(bottom);
        return createCell(wrapper);
    };


    // Display progressively the data
    const displayData = () => {
        const tableBody = document.getElementById('itemsTableBody');
        const endIndex = currentIndex + batchSize;
        const currentBatch = filteredData.slice(currentIndex, endIndex);

        currentBatch.forEach((item, index) => {
            const euroPrice = (item.price * yenToEuroMultiplier).toFixed(2);
            const originalPrice = item.full_price || item.price;
            const originalEuroPrice = (originalPrice * yenToEuroMultiplier).toFixed(2);
            const discountPercent = getDiscountPercent(item).toFixed(1);
            const releaseDate = item.release_date ? new Date(item.release_date).toLocaleDateString('en-GB', { year: 'numeric', month: 'short', day: 'numeric' }) : defaultValue;

            const row = document.createElement('tr');

            const image = document.createElement('img');
            image.src = item.image_url;
            image.alt = item.name;
            image.loading = 'lazy';

            const jancodeCell = item.jancode
                ? createCell(createLink(`https://myfigurecollection.net/?keywords=${item.jancode}&_tb=item`, item.jancode))
                : createCell(defaultValue);

            row.appendChild(createCell(`${index + currentIndex + 1} / ${filteredData.length}`));
            row.appendChild(createCell(image));
            row.appendChild(createCell(item.name));
            row.appendChild(createSplitCell(
                createLink(item.gcode_url, item.gcode),
                createLink(item.scode_url, item.scode || defaultValue),
            ));
            row.appendChild(createSplitCell(`¥${item.price}`, `${euroPrice} €`));
            row.appendChild(createSplitCell(`¥${originalPrice}`, `${originalEuroPrice} €`));
            row.appendChild(createCell(`${discountPercent}%`));
            row.appendChild(createCell(item.sale_status || defaultValue));
            row.appendChild(createCell(releaseDate));
            row.appendChild(jancodeCell);
            row.appendChild(createSplitCell(
                item.item_condition ? `ITEM: ${item.item_condition}` : 'New',
                item.box_condition ? `BOX: ${item.box_condition}` : 'New',
            ));

            tableBody.appendChild(row);
        });

        currentIndex = endIndex;

        // Update item count
        updateDisplayedCount();
    };


    // Update count of currently displayed items
    const updateDisplayedCount = () => {
        const countDisplay = document.getElementById('itemsCount');
        countDisplay.textContent = filteredData.length;
    };


    // Scroll handling to paginate items
    const handleScroll = () => {
        // Triggered before end of page
        const nearBottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 200;
        if (nearBottom && currentIndex < filteredData.length) {
            displayData();
        }
    };


    // Data filtering
    const filterData = () => {
        return jsonData.filter(item => {
            const query = document.getElementById('searchInput').value.toLowerCase();

            // Checkbox filters
            const itemConditions = Array.from(document.querySelectorAll('input[name="item_condition"]:checked')).map(el => el.value);
            const boxConditions = Array.from(document.querySelectorAll('input[name="box_condition"]:checked')).map(el => el.value);
            const itemBoolDetails = Array.from(document.querySelectorAll('input[name="item_bool_details"]:checked')).map(el => el.value);

            // Number filters
            const minPrice = parseFloat(minPriceInput.value.replace(',', '.')) || 0.0;
            const maxPrice = parseFloat(maxPriceInput.value.replace(',', '.')) || Infinity;
            const euroPrice = (item.price * yenToEuroMultiplier).toFixed(2);

            // Query filter
            const matchesText = item.name.toLowerCase().includes(query)
                || item.gcode.toLowerCase().includes(query)
                || item.scode.toLowerCase().includes(query)
                || item.jancode?.toLowerCase().includes(query)
                || item.tags.some(tag => tag.toLowerCase().includes(query))
                || item.maker_name.toLowerCase().includes(query)
                || item.modeler_name.toLowerCase().includes(query)
                || item.description.toLowerCase().includes(query);

            // Check conditions

            const matchesItemCondition = itemConditions.length === 0 || itemConditions.includes(item.item_condition);
            const matchesBoxCondition = boxConditions.length === 0 || boxConditions.includes(item.box_condition);
            const matchesBool = itemBoolDetails.length === 0 || itemBoolDetails.every(filter => item[filter] === true);

            const matchesPrice = euroPrice >= minPrice && euroPrice <= maxPrice;

            return matchesText && matchesItemCondition && matchesBoxCondition && matchesBool && matchesPrice;
        });
    };


    // Sorting application
    const sortData = (column, direction) => {
        filteredData.sort((a, b) => {
            let valueA = a[column];
            let valueB = b[column];

            if (column === 'release_date') {
                valueA = new Date(a.release_date);
                valueB = new Date(b.release_date);
            } else if (column === 'discount_percent') {
                valueA = getDiscountPercent(a);
                valueB = getDiscountPercent(b);
            } else if (column === 'item_condition') {
                valueA = itemConditionOrder.indexOf(a.item_condition);
                valueB = itemConditionOrder.indexOf(b.item_condition);

                // Case when item condition is unknown
                if (valueA === -1) valueA = itemConditionOrder.length;
                if (valueB === -1) valueB = itemConditionOrder.length;
            }

            if (direction === 'asc') {
                return valueA > valueB ? 1 : valueA < valueB ? -1 : 0;
            } else {
                return valueA < valueB ? 1 : valueA > valueB ? -1 : 0;
            }
        });

        // Reload data after sorting
        document.getElementById('itemsTableBody').innerHTML = '';
        currentIndex = 0;
        displayData();
    };


    // Setup sorting on given column
    const setupSort = (column, element) => {
        const sortDirection = currentSort.column === column && currentSort.direction === 'asc' ? 'desc' : 'asc';
        currentSort = { column, direction: sortDirection };

        // Apply sorting options
        sortData(column, sortDirection);

        // Update visual elements
        document.querySelectorAll('.sortable').forEach(header => header.classList.remove('sorted-asc', 'sorted-desc'));
        element.classList.add(sortDirection === 'asc' ? 'sorted-asc' : 'sorted-desc');
    };


    // Refresh view
    const refreshView = () => {
        filteredData = filterData();
        currentIndex = 0;
        document.getElementById('itemsTableBody').innerHTML = '';
        displayData();
    };


    // Search input management
    let searchInputTimeoutId;
    const searchInput = document.getElementById('searchInput');
    searchInput.addEventListener('input', () => {
        clearTimeout(searchInputTimeoutId);

        searchInputTimeoutId = setTimeout(() => {
            refreshView();
        }, timeoutValue);
    });


    // Checkbox filters management
    const filterCheckboxes = document.querySelectorAll('input[type="checkbox"]');
    filterCheckboxes.forEach(checkbox => {
        checkbox.addEventListener('change', () => {
            refreshView();
        });
    });


    // Price management
    let minPriceTimeoutId;
    minPriceInput.addEventListener('input', () => {
        clearTimeout(minPriceTimeoutId);

        minPriceTimeoutId = setTimeout(() => {
            refreshView();
        }, timeoutValue);
    });

    let maxPriceTimeoutId;
    maxPriceInput.addEventListener('input', () => {
        clearTimeout(maxPriceTimeoutId);

        maxPriceTimeoutId = setTimeout(() => {
            refreshView();
        }, timeoutValue);
    });


    // Initial loading
    loadData();

    // Scroll manager
    window.addEventListener('scroll', handleScroll);

    // Initialize sorting
    document.querySelectorAll('.sortable').forEach(header => {
        header.addEventListener('click', () => {
            const column = header.dataset.column;
            setupSort(column, header);
        });
    });
});
