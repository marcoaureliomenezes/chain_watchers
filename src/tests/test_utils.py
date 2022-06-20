# def test_case(size):
#     # test_cases = [(i + 1) /size for i in range(size)]
#     test_cases = [0.5]
#     print(f"TEST CASES:\n{test_cases}\n")
#     for scenario in test_cases:
#         test_array = create_test_arr(size, (30, 35), scenario)
#         print(f"Array de teste:\n{test_array}\n")

#         entrada = {
#             'top': test_array[0],
#             'top_minus1': (test_array[0][0]-1, mock_get_round_id(test_array, test_array[0][0]-1)[1]) ,
#             'bottom': (1, mock_get_round_id(test_array, 1)[1]),
#             'bottom_minus1': (0, mock_get_round_id(test_array, 0)[1])
#         }

#         print(find_begin(test_array, entrada))