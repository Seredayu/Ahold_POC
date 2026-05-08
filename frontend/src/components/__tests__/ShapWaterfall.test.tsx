import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ShapWaterfall } from '../ShapWaterfall'

const FIXTURE_SHAP = {
  demand_velocity: 0.15,
  freshness_index: -0.05,
  transit_to_life_ratio: 0.08,
  promotion_active: 0.10,
}

describe('ShapWaterfall', () => {
  it('renders title with SKU and site', () => {
    render(<ShapWaterfall shapValues={FIXTURE_SHAP} baseValue={0} predictedValue={45} skuId="SKU123" siteId="AH01" />)
    expect(screen.getByText(/Why this order/)).toBeInTheDocument()
    expect(screen.getByText(/SKU123/)).toBeInTheDocument()
    expect(screen.getByText(/AH01/)).toBeInTheDocument()
  })

  it('renders correct number of feature bars', () => {
    render(<ShapWaterfall shapValues={FIXTURE_SHAP} baseValue={0} predictedValue={45} skuId="SKU123" siteId="AH01" />)
    // 4 features in fixture
    expect(document.querySelectorAll('.shap-bar').length).toBe(4)
  })

  it('applies positive class for positive values', () => {
    render(<ShapWaterfall shapValues={{ demand_velocity: 0.15 }} baseValue={0} predictedValue={45} skuId="SKU123" siteId="AH01" />)
    expect(document.querySelector('.shap-bar.positive')).toBeTruthy()
  })

  it('applies negative class for negative values', () => {
    render(<ShapWaterfall shapValues={{ freshness_index: -0.05 }} baseValue={0} predictedValue={45} skuId="SKU123" siteId="AH01" />)
    expect(document.querySelector('.shap-bar.negative')).toBeTruthy()
  })

  it('renders empty state when shapValues is empty', () => {
    render(<ShapWaterfall shapValues={{}} baseValue={0} predictedValue={45} skuId="SKU123" siteId="AH01" />)
    expect(screen.getByText(/No SHAP data available/)).toBeInTheDocument()
  })

  it('renders predicted value in units', () => {
    render(<ShapWaterfall shapValues={FIXTURE_SHAP} baseValue={0} predictedValue={45} skuId="SKU123" siteId="AH01" />)
    expect(screen.getByText(/45 units/)).toBeInTheDocument()
  })
})
