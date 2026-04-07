import sys
import json
import heapq

def calculate_chi_square():
    # 1. Get category totals
    input_file = sys.argv[1]

    cat_totals = {}
    with open(input_file, 'r') as f:
        for line in f:
            if "__TOTAL__" in line:
                key, count = line.split('\t')
                _, cat = json.loads(key)
                cat_totals[cat] = int(count)

    total_n = sum(cat_totals.values())
    all_categories = sorted(cat_totals.keys())
    
    top_heaps = {cat: [] for cat in all_categories}
    
    # 2. Process terms in loop
    current_term = None
    term_cat_counts = {}

    with open(input_file, 'r') as f:
        for line in f:
            if not line.strip() or "__TOTAL__" in line:
                continue
            
            key, count = line.split('\t')
            term, cat = json.loads(key)
            count = int(count)

            if term != current_term:
                if current_term is not None:
                    # Calculate Chi-Square for the finished term
                    process_term(current_term, term_cat_counts, cat_totals, total_n, top_heaps)
                
                current_term = term
                term_cat_counts = {}
            
            term_cat_counts[cat] = count

        if current_term:
            process_term(current_term, term_cat_counts, cat_totals, total_n, top_heaps)

    # 3. Output Formatting
    all_top_terms = set()
    for cat in all_categories:
        top_75 = heapq.nlargest(75, top_heaps[cat])
        for _, t in top_75:
            all_top_terms.add(t)
        
        formatted = " ".join([f"{t}:{round(s, 4)}" for s, t in top_75])
        print(f"{cat} {formatted}")

    print(" ".join(sorted(list(all_top_terms))))

def process_term(term, counts, cat_totals, n, top_heaps):
    total_term_freq = sum(counts.values())
    for cat, n_cat in cat_totals.items():
        a = counts.get(cat, 0)
        b = total_term_freq - a
        c = n_cat - a
        d = n - (a + b + c)
        
        num = n * ((a * d - b * c) ** 2)
        den = (a + b) * (c + d) * (a + c) * (b + d)
        chi = num / den if den != 0 else 0
        
        # Keep only top 75 in the heap to save memory
        if len(top_heaps[cat]) < 75:
            heapq.heappush(top_heaps[cat], (chi, term))
        else:
            heapq.heappushpop(top_heaps[cat], (chi, term))

if __name__ == "__main__":
    calculate_chi_square()