p_a = 0.7
p_b = p_a * 0.5
p_c = p_b * 0.75

prop_a = 0.6
prop_b = 0.3
prop_c = 0.1

prop_total = (prop_a * p_a) + (prop_b * p_b) + (prop_c * p_c)

print(f"A probabilidade de um cliente escolhido ao acaso adquirir o produto é {prop_total:.4f} ou {prop_total*100:.2f}")
