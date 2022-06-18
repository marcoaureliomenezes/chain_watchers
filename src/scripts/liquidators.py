


# Liquidation call

    # @dev Emitted when a borrower is liquidated.
    # @param collateralAsset The address of the underlying asset used as collateral, to receive as result of the liquidation
    # @param debtAsset The address of the underlying borrowed asset to be repaid with the liquidation
    # @param user The address of the borrower getting liquidated
    # @param debtToCover The debt amount of borrowed `asset` the liquidator wants to cover
    # @param liquidatedCollateralAmount The amount of collateral received by the liquidator
    # @param liquidator The address of the liquidator
    # @param receiveAToken True if the liquidators wants to receive the collateral aTokens, `false` if he wants
    # to receive the underlying collateral asset directly


# Flash Loan

    # @notice Allows smartcontracts to access the liquidity of the pool within one transaction,
    # as long as the amount taken plus a fee is returned.
    # @dev IMPORTANT There are security concerns for developers of flashloan receiver contracts that must be kept
    # into consideration. For further details please visit https://developers.aave.com
    # @param receiverAddress The address of the contract receiving the funds, implementing IFlashLoanReceiver interface
    # @param assets The addresses of the assets being flash-borrowed
    # @param amounts The amounts of the assets being flash-borrowed
    # @param interestRateModes Types of the debt to open if the flash loan is not returned:
    #   0 -> Don't open any debt, just revert if funds can't be transferred from the receiver
    #   1 -> Open debt at stable rate for the value of the amount flash-borrowed to the `onBehalfOf` address
    #   2 -> Open debt at variable rate for the value of the amount flash-borrowed to the `onBehalfOf` address
    # @param onBehalfOf The address  that will receive the debt in the case of using on `modes` 1 or 2
    # @param params Variadic packed params to pass to the receiver as extra information
    # @param referralCode The code used to register the integrator originating the operation, for potential rewards.
    #   0 if the action is executed directly by the user, without any middle-man
    