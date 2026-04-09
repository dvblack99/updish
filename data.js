// UpDish — sample dataset
// Source: City of Vancouver Open Data (business licences, food service)
// In production, replace with live fetch from:
// https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/business-licences/records
// filtered by: BusinessType in ('Restaurant','Cafeteria','Coffee Bar/Tea House','Caterer','Food Processor','Pub','Tavern/Lounge')
// Geocoding: addresses pre-geocoded via Google Maps Geocoding API, cached here

const BUSINESSES = [
  // ── Kitsilano ──────────────────────────────────────────────────────────────
  { id: 1, name: "Corvo Coffee", address: "1842 W 4th Ave", neighbourhood: "Kitsilano", type: "cafe", employees: 4, licence: "Food Primary", status: "active", lat: 49.2668, lng: -123.1540 },
  { id: 2, name: "Aphrodite's Organic Café", address: "3082 W Broadway", neighbourhood: "Kitsilano", type: "cafe", employees: 8, licence: "Food Primary", status: "active", lat: 49.2635, lng: -123.1703 },
  { id: 3, name: "Nook", address: "1525 Yew St", neighbourhood: "Kitsilano", type: "restaurant", employees: 14, licence: "Food Primary", status: "active", lat: 49.2692, lng: -123.1573 },
  { id: 4, name: "Soirette Macarons & Tea", address: "1433 W 16th Ave", neighbourhood: "Kitsilano", type: "cafe", employees: 5, licence: "Food Primary", status: "active", lat: 49.2530, lng: -123.1474 },
  { id: 5, name: "Fable Kitchen", address: "1944 W 4th Ave", neighbourhood: "Kitsilano", type: "restaurant", employees: 22, licence: "Food Primary", status: "active", lat: 49.2671, lng: -123.1567 },
  { id: 6, name: "Miku Sushi Kits", address: "2070 W 4th Ave", neighbourhood: "Kitsilano", type: "restaurant", employees: 18, licence: "Food Primary", status: "active", lat: 49.2673, lng: -123.1590 },
  { id: 7, name: "Grounds for Coffee", address: "2005 W 4th Ave", neighbourhood: "Kitsilano", type: "cafe", employees: 6, licence: "Food Primary", status: "active", lat: 49.2672, lng: -123.1579 },
  { id: 8, name: "Beaucoup Bakery Kits", address: "2150 Fir St", neighbourhood: "Kitsilano", type: "cafe", employees: 9, licence: "Food Primary", status: "active", lat: 49.2652, lng: -123.1421 },
  { id: 9, name: "Kits Beach Grill", address: "1499 Arbutus St", neighbourhood: "Kitsilano", type: "restaurant", employees: 12, licence: "Food Primary", status: "active", lat: 49.2695, lng: -123.1535 },
  { id: 10, name: "49th Parallel — Kits", address: "2902 W Broadway", neighbourhood: "Kitsilano", type: "cafe", employees: 7, licence: "Food Primary", status: "active", lat: 49.2634, lng: -123.1669 },
  { id: 11, name: "Chau Veggie Express", address: "2006 W 4th Ave", neighbourhood: "Kitsilano", type: "restaurant", employees: 8, licence: "Food Primary", status: "active", lat: 49.2672, lng: -123.1580 },
  { id: 12, name: "Kishimoto Japanese Kits", address: "2054 W 4th Ave", neighbourhood: "Kitsilano", type: "restaurant", employees: 10, licence: "Food Primary", status: "active", lat: 49.2673, lng: -123.1588 },
  { id: 13, name: "Kits Coffee", address: "3598 W 4th Ave", neighbourhood: "Kitsilano", type: "cafe", employees: 3, licence: "Food Primary", status: "active", lat: 49.2668, lng: -123.1849 },
  { id: 14, name: "The Naam", address: "2724 W 4th Ave", neighbourhood: "Kitsilano", type: "restaurant", employees: 25, licence: "Food Primary", status: "active", lat: 49.2667, lng: -123.1638 },
  { id: 15, name: "Prado Café", address: "1938 Cornwall Ave", neighbourhood: "Kitsilano", type: "cafe", employees: 5, licence: "Food Primary", status: "active", lat: 49.2718, lng: -123.1545 },

  // ── Mount Pleasant ─────────────────────────────────────────────────────────
  { id: 16, name: "Matchstick Coffee — Main", address: "639 E 15th Ave", neighbourhood: "Mount Pleasant", type: "cafe", employees: 9, licence: "Food Primary", status: "active", lat: 49.2509, lng: -123.1009 },
  { id: 17, name: "Lucky Taco", address: "826 Main St", neighbourhood: "Mount Pleasant", type: "restaurant", employees: 7, licence: "Food Primary", status: "active", lat: 49.2577, lng: -123.1003 },
  { id: 18, name: "Earnest Ice Cream", address: "1829 Quebec St", neighbourhood: "Mount Pleasant", type: "cafe", employees: 6, licence: "Food Primary", status: "active", lat: 49.2617, lng: -123.1019 },
  { id: 19, name: "Burdock & Co", address: "2702 Main St", neighbourhood: "Mount Pleasant", type: "restaurant", employees: 16, licence: "Food Primary", status: "active", lat: 49.2472, lng: -123.1008 },
  { id: 20, name: "Anh & Chi", address: "3388 Main St", neighbourhood: "Mount Pleasant", type: "restaurant", employees: 20, licence: "Food Primary", status: "active", lat: 49.2388, lng: -123.1007 },
  { id: 21, name: "Propaganda Coffee", address: "209 E Broadway", neighbourhood: "Mount Pleasant", type: "cafe", employees: 4, licence: "Food Primary", status: "active", lat: 49.2628, lng: -123.0952 },
  { id: 22, name: "Kafka's Coffee", address: "2525 Main St", neighbourhood: "Mount Pleasant", type: "cafe", employees: 5, licence: "Food Primary", status: "active", lat: 49.2508, lng: -123.1007 },
  { id: 23, name: "The Acorn", address: "3995 Main St", neighbourhood: "Mount Pleasant", type: "restaurant", employees: 14, licence: "Food Primary", status: "active", lat: 49.2346, lng: -123.1007 },
  { id: 24, name: "Bells & Whistles", address: "2441 E Hastings St", neighbourhood: "Mount Pleasant", type: "restaurant", employees: 12, licence: "Food Primary", status: "active", lat: 49.2808, lng: -123.0502 },
  { id: 25, name: "Rosarium Café", address: "2901 Main St", neighbourhood: "Mount Pleasant", type: "cafe", employees: 3, licence: "Food Primary", status: "active", lat: 49.2449, lng: -123.1008 },
  { id: 26, name: "Bestie", address: "1085 Drake St", neighbourhood: "Mount Pleasant", type: "restaurant", employees: 8, licence: "Food Primary", status: "active", lat: 49.2762, lng: -123.1298 },
  { id: 27, name: "Tangent Café", address: "2095 Commercial Dr", neighbourhood: "Mount Pleasant", type: "cafe", employees: 6, licence: "Food Primary", status: "active", lat: 49.2580, lng: -123.0696 },

  // ── Downtown / Yaletown ────────────────────────────────────────────────────
  { id: 28, name: "Revolver Coffee", address: "325 Cambie St", neighbourhood: "Downtown", type: "cafe", employees: 8, licence: "Food Primary", status: "active", lat: 49.2834, lng: -123.1083 },
  { id: 29, name: "Hawksworth Restaurant", address: "801 W Georgia St", neighbourhood: "Downtown", type: "restaurant", employees: 60, licence: "Food Primary", status: "active", lat: 49.2839, lng: -123.1204 },
  { id: 30, name: "Pidgin", address: "350 Carrall St", neighbourhood: "Downtown", type: "restaurant", employees: 18, licence: "Food Primary", status: "active", lat: 49.2830, lng: -123.1045 },
  { id: 31, name: "Timber", address: "1300 Robson St", neighbourhood: "Downtown", type: "restaurant", employees: 30, licence: "Liquor Primary", status: "active", lat: 49.2861, lng: -123.1303 },
  { id: 32, name: "Catch 122", address: "122 W Hastings St", neighbourhood: "Downtown", type: "cafe", employees: 12, licence: "Food Primary", status: "active", lat: 49.2835, lng: -123.1069 },
  { id: 33, name: "Jamjar", address: "1099 Davie St", neighbourhood: "Downtown", type: "restaurant", employees: 10, licence: "Food Primary", status: "active", lat: 49.2795, lng: -123.1314 },
  { id: 34, name: "Ask for Luigi", address: "305 Alexander St", neighbourhood: "Downtown", type: "restaurant", employees: 20, licence: "Food Primary", status: "active", lat: 49.2849, lng: -123.0967 },
  { id: 35, name: "Medina Café", address: "780 Richards St", neighbourhood: "Downtown", type: "cafe", employees: 22, licence: "Food Primary", status: "active", lat: 49.2810, lng: -123.1200 },
  { id: 36, name: "Elisa", address: "1285 W Pender St", neighbourhood: "Downtown", type: "restaurant", employees: 16, licence: "Food Primary", status: "active", lat: 49.2862, lng: -123.1255 },
  { id: 37, name: "Tuc Craft Kitchen", address: "60 W Cordova St", neighbourhood: "Downtown", type: "restaurant", employees: 18, licence: "Liquor Primary", status: "active", lat: 49.2833, lng: -123.1057 },
  { id: 38, name: "Breka Bakery — Robson", address: "812 Bute St", neighbourhood: "Downtown", type: "cafe", employees: 15, licence: "Food Primary", status: "active", lat: 49.2850, lng: -123.1256 },
  { id: 39, name: "Bauhaus Restaurant", address: "1 W Cordova St", neighbourhood: "Downtown", type: "restaurant", employees: 24, licence: "Liquor Primary", status: "active", lat: 49.2837, lng: -123.1048 },

  // ── Commercial Drive ───────────────────────────────────────────────────────
  { id: 40, name: "Prado Café — Commercial", address: "1121 Commercial Dr", neighbourhood: "Commercial Drive", type: "cafe", employees: 6, licence: "Food Primary", status: "active", lat: 49.2729, lng: -123.0692 },
  { id: 41, name: "Havana Restaurant", address: "1212 Commercial Dr", neighbourhood: "Commercial Drive", type: "restaurant", employees: 20, licence: "Liquor Primary", status: "active", lat: 49.2713, lng: -123.0691 },
  { id: 42, name: "Falconetti's", address: "1812 Commercial Dr", neighbourhood: "Commercial Drive", type: "restaurant", employees: 10, licence: "Liquor Primary", status: "active", lat: 49.2608, lng: -123.0688 },
  { id: 43, name: "The Grind Coffee Bar", address: "4124 Main St", neighbourhood: "Commercial Drive", type: "cafe", employees: 4, licence: "Food Primary", status: "active", lat: 49.2318, lng: -123.1007 },
  { id: 44, name: "Caffe Calabria", address: "1745 Commercial Dr", neighbourhood: "Commercial Drive", type: "cafe", employees: 5, licence: "Food Primary", status: "active", lat: 49.2620, lng: -123.0689 },
  { id: 45, name: "La Casa Gelato", address: "1033 Venables St", neighbourhood: "Commercial Drive", type: "cafe", employees: 12, licence: "Food Primary", status: "active", lat: 49.2740, lng: -123.0700 },
  { id: 46, name: "Simba's Grill", address: "1531 Commercial Dr", neighbourhood: "Commercial Drive", type: "restaurant", employees: 8, licence: "Food Primary", status: "active", lat: 49.2651, lng: -123.0689 },
  { id: 47, name: "Bump N Grind", address: "916 Commercial Dr", neighbourhood: "Commercial Drive", type: "cafe", employees: 4, licence: "Food Primary", status: "active", lat: 49.2754, lng: -123.0694 },
  { id: 48, name: "Bin 942 Commercial", address: "1521 W Broadway", neighbourhood: "Commercial Drive", type: "restaurant", employees: 15, licence: "Liquor Primary", status: "active", lat: 49.2634, lng: -123.1443 },

  // ── Fairview / South Granville ─────────────────────────────────────────────
  { id: 49, name: "Trafiq Café", address: "1100 Granville St", neighbourhood: "Fairview", type: "cafe", employees: 5, licence: "Food Primary", status: "active", lat: 49.2769, lng: -123.1242 },
  { id: 50, name: "Vij's", address: "3106 Cambie St", neighbourhood: "Fairview", type: "restaurant", employees: 28, licence: "Food Primary", status: "active", lat: 49.2562, lng: -123.1153 },
  { id: 51, name: "Seasons in the Park", address: "Queen Elizabeth Park", neighbourhood: "Fairview", type: "restaurant", employees: 35, licence: "Food Primary", status: "active", lat: 49.2413, lng: -123.1133 },
  { id: 52, name: "Café Crepe", address: "740 Denman St", neighbourhood: "Fairview", type: "cafe", employees: 9, licence: "Food Primary", status: "active", lat: 49.2875, lng: -123.1392 },
  { id: 53, name: "Zakkushi Charcoal Grill", address: "823 Denman St", neighbourhood: "Fairview", type: "restaurant", employees: 12, licence: "Liquor Primary", status: "active", lat: 49.2868, lng: -123.1388 },
  { id: 54, name: "Ramen Butcher", address: "256 E Broadway", neighbourhood: "Fairview", type: "restaurant", employees: 10, licence: "Food Primary", status: "active", lat: 49.2629, lng: -123.0939 },
  { id: 55, name: "Chambar", address: "568 Beatty St", neighbourhood: "Fairview", type: "restaurant", employees: 40, licence: "Liquor Primary", status: "active", lat: 49.2797, lng: -123.1132 },

  // ── East Van / Hastings-Sunrise ────────────────────────────────────────────
  { id: 56, name: "Bon's Off Broadway", address: "2451 Nanaimo St", neighbourhood: "East Vancouver", type: "restaurant", employees: 8, licence: "Food Primary", status: "active", lat: 49.2578, lng: -123.0601 },
  { id: 57, name: "Café Deux Soleils", address: "2096 Commercial Dr", neighbourhood: "East Vancouver", type: "cafe", employees: 7, licence: "Food Primary", status: "active", lat: 49.2578, lng: -123.0695 },
  { id: 58, name: "Union Market", address: "1072 Main St", neighbourhood: "East Vancouver", type: "cafe", employees: 5, licence: "Food Primary", status: "active", lat: 49.2763, lng: -123.1002 },
  { id: 59, name: "East is East", address: "3243 W Broadway", neighbourhood: "East Vancouver", type: "restaurant", employees: 14, licence: "Food Primary", status: "active", lat: 49.2636, lng: -123.1729 },
  { id: 60, name: "Juke Fried Chicken", address: "182 Keefer St", neighbourhood: "East Vancouver", type: "restaurant", employees: 12, licence: "Food Primary", status: "active", lat: 49.2792, lng: -123.1012 },
  { id: 61, name: "Save-On Meats", address: "43 W Hastings St", neighbourhood: "East Vancouver", type: "restaurant", employees: 18, licence: "Food Primary", status: "active", lat: 49.2823, lng: -123.1053 },
  { id: 62, name: "Phnom Penh Restaurant", address: "244 E Georgia St", neighbourhood: "East Vancouver", type: "restaurant", employees: 20, licence: "Food Primary", status: "active", lat: 49.2804, lng: -123.1008 },

  // ── West End ───────────────────────────────────────────────────────────────
  { id: 63, name: "Café Pacifica", address: "1119 W Pender St", neighbourhood: "West End", type: "cafe", employees: 6, licence: "Food Primary", status: "active", lat: 49.2866, lng: -123.1267 },
  { id: 64, name: "Forage", address: "1300 Robson St", neighbourhood: "West End", type: "restaurant", employees: 25, licence: "Food Primary", status: "active", lat: 49.2861, lng: -123.1303 },
  { id: 65, name: "Raisu Japanese Cuisine", address: "735 Denman St", neighbourhood: "West End", type: "restaurant", employees: 11, licence: "Food Primary", status: "active", lat: 49.2874, lng: -123.1394 },
  { id: 66, name: "Espana Restaurant", address: "1118 Denman St", neighbourhood: "West End", type: "restaurant", employees: 14, licence: "Liquor Primary", status: "active", lat: 49.2855, lng: -123.1391 },
  { id: 67, name: "Cactus Club — English Bay", address: "1790 Beach Ave", neighbourhood: "West End", type: "restaurant", employees: 55, licence: "Liquor Primary", status: "active", lat: 49.2876, lng: -123.1460 },
  { id: 68, name: "The Tuck Shop", address: "1925 W 4th Ave", neighbourhood: "West End", type: "cafe", employees: 4, licence: "Food Primary", status: "active", lat: 49.2670, lng: -123.1568 },

  // ── Gastown / Chinatown ────────────────────────────────────────────────────
  { id: 69, name: "Meat & Bread", address: "370 Cambie St", neighbourhood: "Gastown", type: "restaurant", employees: 9, licence: "Food Primary", status: "active", lat: 49.2831, lng: -123.1083 },
  { id: 70, name: "L'Abattoir", address: "217 Carrall St", neighbourhood: "Gastown", type: "restaurant", employees: 22, licence: "Liquor Primary", status: "active", lat: 49.2832, lng: -123.1054 },
  { id: 71, name: "Nuba Gastown", address: "207 W Hastings St", neighbourhood: "Gastown", type: "restaurant", employees: 15, licence: "Food Primary", status: "active", lat: 49.2831, lng: -123.1074 },
  { id: 72, name: "Portside Pub", address: "7 Alexander St", neighbourhood: "Gastown", type: "bar", employees: 18, licence: "Liquor Primary", status: "active", lat: 49.2849, lng: -123.0993 },
  { id: 73, name: "Bao Bei", address: "163 Keefer St", neighbourhood: "Chinatown", type: "restaurant", employees: 16, licence: "Liquor Primary", status: "active", lat: 49.2793, lng: -123.1016 },
  { id: 74, name: "The Flying Pig", address: "102 Water St", neighbourhood: "Gastown", type: "restaurant", employees: 20, licence: "Liquor Primary", status: "active", lat: 49.2845, lng: -123.1077 },
  { id: 75, name: "Tacofino Gastown", address: "15 W Cordova St", neighbourhood: "Gastown", type: "food_truck", employees: 7, licence: "Food Primary", status: "active", lat: 49.2838, lng: -123.1058 },

  // ── Inactive examples ──────────────────────────────────────────────────────
  { id: 76, name: "The Cascade Room", address: "2616 Main St", neighbourhood: "Mount Pleasant", type: "bar", employees: 12, licence: "Liquor Primary", status: "inactive", lat: 49.2491, lng: -123.1007 },
  { id: 77, name: "Elbow Room Café", address: "560 Davie St", neighbourhood: "Downtown", type: "cafe", employees: 6, licence: "Food Primary", status: "inactive", lat: 49.2780, lng: -123.1264 },
  { id: 78, name: "Diner Deluxe Kits", address: "1842 W Broadway", neighbourhood: "Kitsilano", type: "restaurant", employees: 10, licence: "Food Primary", status: "inactive", lat: 49.2635, lng: -123.1524 },
  { id: 79, name: "Phat Pi Bubble Tea", address: "3210 Kingsway", neighbourhood: "East Vancouver", type: "cafe", employees: 3, licence: "Food Primary", status: "active", lat: 49.2508, lng: -123.0496 },
  { id: 80, name: "Sunrise Dim Sum", address: "2256 E Hastings St", neighbourhood: "East Vancouver", type: "restaurant", employees: 22, licence: "Food Primary", status: "active", lat: 49.2808, lng: -123.0524 }
];

const BENCHMARKS = [
  { stat: "31–34%", desc: "avg food cost ratio for BC independents", source: "Restaurants Canada 2026" },
  { stat: "~62%", desc: "of new food licences survive 3+ years in Vancouver", source: "BC Stats 2025" },
  { stat: "$42k", desc: "median annual revenue per seat, full-service", source: "TouchBistro 2025" },
  { stat: "3.4×", desc: "Kitsilano café density vs city average", source: "CoV Open Data" },
  { stat: "~8%", desc: "net margin for healthy independent café", source: "Restaurants Canada" },
  { stat: "28–32%", desc: "target labour cost as % of revenue", source: "TouchBistro 2025" }
];
