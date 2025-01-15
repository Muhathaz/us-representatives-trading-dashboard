import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from fetch_dashboard_data import DashboardData
import pandas as pd

# Set page config
st.set_page_config(
    page_title="US Representatives Trading Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize data fetcher
@st.cache_resource(ttl=3600)
def get_dashboard_data():
    return DashboardData()

# Get data connection
data = get_dashboard_data()

# Sidebar navigation
page = st.sidebar.selectbox(
    "Select Page",
    ["Representative Analysis", "Stock Analysis"]
)

# Define color mapping for parties
party_colors = {
    'Republican': '#FF0000',  # Red
    'Democrat': '#0000FF',     # Blue
    'Independent': '#808080',  # Gray for Independents or others
}

if page == "Representative Analysis":
    st.title("Representative Analysis")
    
    # Representative selection
    representatives = data.get_all_representatives()
    selected_rep = st.selectbox(
        "Select Representative",
        representatives['name'].tolist()
    )
    
    # Date Range Filter
    portfolio_data = data.get_portfolio_value_analysis(selected_rep)
    if not portfolio_data.empty:
        min_date = pd.to_datetime(portfolio_data['transaction_date'].min())
        max_date = pd.to_datetime(portfolio_data['transaction_date'].max())
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
        with col2:
            end_date = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)
        
        # Filter data based on selected date range
        mask = (portfolio_data['transaction_date'] >= pd.Timestamp(start_date)) & \
               (portfolio_data['transaction_date'] <= pd.Timestamp(end_date))
        filtered_portfolio_data = portfolio_data[mask]
        
        # Portfolio Value Analysis
        st.subheader("Portfolio Value Progression")
        fig = px.area(
            filtered_portfolio_data,
            x='transaction_date',
            y='stock_value',
            color='ticker',
            labels={
                'transaction_date': 'Date',
                'stock_value': 'Value ($)',
                'ticker': 'Stock'
            }
        )
        
        fig.update_layout(
            height=400,
            margin=dict(l=40, r=40, t=40, b=40),
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            yaxis_title="Portfolio Value ($)",
            xaxis_title="Date"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Create two columns for the top row
    col1, col2 = st.columns(2)

    with col1:
        # Trading Overview
        st.subheader("Trading Overview")
        rep_data = data.get_representative_overview(selected_rep)
        if not rep_data.empty:
            metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
            
            metrics_col1.metric("Total Trades", rep_data.iloc[0]['total_trades'])
            metrics_col2.metric("Unique Stocks", rep_data.iloc[0]['unique_stocks'])
            metrics_col3.metric("Years Active", rep_data.iloc[0]['years_active'])
            
            metrics_col1.metric("Purchases", rep_data.iloc[0]['total_purchases'])
            metrics_col2.metric("Sales", rep_data.iloc[0]['total_sales'])
            metrics_col3.metric("Unique Sectors", rep_data.iloc[0]['unique_sectors'])

    with col2:
        # Current Positions
        st.subheader("Current Positions")
        positions_data = data.get_current_positions(selected_rep)
        
        if not positions_data.empty:
            fig_positions = px.bar(
                positions_data,
                y='ticker',
                x='current_value',
                orientation='h',
                color='sector',  # Use sector for color
                title="Open Positions by Value",
                height=600,
                labels={
                    'current_value': 'Position Value ($)',
                    'ticker': 'Stock',
                    'sector': 'Sector'
                }
            )
            fig_positions.update_layout(
                showlegend=True,
                yaxis={'categoryorder': 'total ascending'},
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_positions, use_container_width=True)
        else:
            st.info("No current positions found for this representative.")

    # Create two columns for the bottom row
    col3, col4 = st.columns(2)

    with col3:
        # Sector Analysis
        st.subheader("Sector Analysis")
        sector_data = data.get_representative_sector_analysis(selected_rep)
        if not sector_data.empty:
            fig_sector = px.pie(
                sector_data,
                values='transaction_count',
                names='sector',
                title="Trading Volume by Sector",
                height=300
            )
            fig_sector.update_traces(textposition='inside', textinfo='percent+label')
            fig_sector.update_layout(margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_sector, use_container_width=True)

    with col4:
        # Trading Activity Timeline
        st.subheader("Trading Activity Timeline")
        timeline_data = data.get_trading_timeline()
        rep_timeline = timeline_data[timeline_data['representative'] == selected_rep]
        
        if not rep_timeline.empty:
            daily_trades = rep_timeline.groupby('transaction_date').size().reset_index(name='trades')
            fig_timeline = px.line(
                daily_trades,
                x='transaction_date',
                y='trades',
                title="Daily Trading Activity",
                height=300
            )
            fig_timeline.update_layout(
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis_title="Date",
                yaxis_title="Number of Trades"
            )
            st.plotly_chart(fig_timeline, use_container_width=True)

elif page == "Stock Analysis":
    st.title("Stock Analysis")
    
    # Stock selection
    stocks = data.get_all_stocks()
    if stocks.empty:
        st.error("Unable to fetch stocks data. Please check the database connection.")
    else:
        selected_stock = st.selectbox(
            "Select Stock",
            stocks['ticker'].tolist()
        )
        
        # Date Range Filter
        stock_price_data = data.get_stock_prices(selected_stock)
        if stock_price_data.empty:
            st.warning(f"No price data available for {selected_stock}")
        else:
            min_date = pd.to_datetime(stock_price_data['date'].min())
            max_date = pd.to_datetime(stock_price_data['date'].max())
            
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
            with col2:
                end_date = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)
            
            # Filter data based on selected date range
            mask = (stock_price_data['date'] >= pd.Timestamp(start_date)) & \
                   (stock_price_data['date'] <= pd.Timestamp(end_date))
            filtered_price_data = stock_price_data[mask]
            
            # Price Chart with Trade Points
            st.subheader("Stock Price and Trading Activity")
            
            # Get price data and trades data
            price_data = data.get_stock_prices(selected_stock)
            trades_data = data.get_stock_trading_timeline(selected_stock)
            
            if not price_data.empty:
                # Filter price data based on selected date range
                mask = (price_data['date'] >= pd.Timestamp(start_date)) & \
                       (price_data['date'] <= pd.Timestamp(end_date))
                filtered_price_data = price_data[mask]
                
                # Create the figure
                fig_price = go.Figure()
                
                # Add price line
                fig_price.add_trace(go.Scatter(
                    x=filtered_price_data['date'],
                    y=filtered_price_data['close'],
                    name='Stock Price',
                    line=dict(color='#0066FF', width=1),
                    hovertemplate="<br>".join([
                        "Date: %{x}",
                        "Price: $%{y:.2f}",
                    ])
                ))
                
                # Add trade bubbles if available
                if not trades_data.empty:
                    trades_in_range = trades_data[
                        (trades_data['transaction_date'] >= pd.Timestamp(start_date)) & 
                        (trades_data['transaction_date'] <= pd.Timestamp(end_date))
                    ]
                    
                    # Add purchase trades
                    purchases = trades_in_range[trades_in_range['type'] == 'purchase']
                    if not purchases.empty:
                        fig_price.add_trace(go.Scatter(
                            x=purchases['transaction_date'],
                            y=purchases['price_at_trade'],
                            mode='markers',
                            name='Purchases',
                            marker=dict(
                                size=10,
                                color='#00CC66',
                                symbol='circle',
                                line=dict(color='#004D26', width=1)
                            ),
                            hovertemplate="<br>".join([
                                "Date: %{x}",
                                "Price: $%{y:.2f}",
                                "Representative: %{customdata[0]}",
                                "Amount: %{customdata[1]}"
                            ]),
                            customdata=purchases[['representative', 'amount']]
                        ))
                    
                    # Add sale trades
                    sales = trades_in_range[trades_in_range['type'] == 'sale']
                    if not sales.empty:
                        fig_price.add_trace(go.Scatter(
                            x=sales['transaction_date'],
                            y=sales['price_at_trade'],
                            mode='markers',
                            name='Sales',
                            marker=dict(
                                size=10,
                                color='#FF3333',
                                symbol='circle',
                                line=dict(color='#990000', width=1)
                            ),
                            hovertemplate="<br>".join([
                                "Date: %{x}",
                                "Price: $%{y:.2f}",
                                "Representative: %{customdata[0]}",
                                "Amount: %{customdata[1]}"
                            ]),
                            customdata=sales[['representative', 'amount']]
                        ))
                
                # Update layout
                fig_price.update_layout(
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=40),
                    hovermode='x unified',
                    yaxis=dict(
                        title="Price ($)",
                        tickformat="$,.2f",
                        side="left",
                        showgrid=True,
                        gridcolor='rgba(128,128,128,0.2)',
                    ),
                    xaxis=dict(
                        title="Date",
                        showgrid=True,
                        gridcolor='rgba(128,128,128,0.2)',
                    ),
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    showlegend=True,
                    legend=dict(
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=0.01,
                        bgcolor='rgba(255,255,255,0.8)'
                    )
                )
                
                st.plotly_chart(fig_price, use_container_width=True)
            else:
                st.warning(f"No price data available for {selected_stock}")
            
            # Create two columns for the middle row
            col1, col2 = st.columns(2)
            
            with col1:
                # Trading Overview
                st.subheader("Trading Overview")
                stock_data = data.get_stock_overview(selected_stock)
                if not stock_data.empty:
                    metrics_col1, metrics_col2 = st.columns(2)
                    
                    metrics_col1.metric("Total Trades", stock_data.iloc[0]['total_trades'])
                    metrics_col2.metric("Active Representatives", stock_data.iloc[0]['active_representatives'])
                    
                    metrics_col1.metric("Avg Holding Duration (days)", 
                                      f"{stock_data.iloc[0]['avg_holding_days']:.1f}")
                    metrics_col2.metric("Total Volume", 
                                      f"{stock_data.iloc[0]['total_volume']:,.0f}")
                else:
                    st.warning("No trading overview data available")
            
            with col2:
                # Current Holdings by Representatives
                st.subheader("Current Holdings by Representatives")
                positions_data = data.get_stock_positions(selected_stock)
                if not positions_data.empty:
                    fig_positions = px.bar(
                        positions_data,
                        y='representative',
                        x='position_value',
                        orientation='h',
                        color='party',  # Use party for color
                        color_discrete_map=party_colors,  # Apply color mapping
                        title="Representatives with Active Positions",
                        height=300,
                        labels={
                            'position_value': 'Position Value ($)',
                            'representative': 'Representative',
                            'party': 'Party'
                        }
                    )
                    fig_positions.update_layout(
                        showlegend=True,
                        yaxis={'categoryorder': 'total ascending'},
                        margin=dict(l=20, r=20, t=40, b=20)
                    )
                    st.plotly_chart(fig_positions, use_container_width=True)
                else:
                    st.warning("No current holdings data available")
            
            # Create two columns for the bottom row
            col3, col4 = st.columns(2)
            
            with col3:
                # Party Distribution
                st.subheader("Party Distribution")
                if not positions_data.empty:
                    party_data = positions_data.groupby('party')['position_value'].sum().reset_index()
                    
                    # Update the color mapping for the pie chart
                    party_colors = {
                        'Republican': '#FF0000',  # Red
                        'Democrat': '#0000FF',     # Blue
                    }
                    
                    fig_party = px.pie(
                        party_data,
                        values='position_value',
                        names='party',
                        title="Holdings by Party",
                        height=300,
                        color_discrete_map=party_colors  # Apply updated color mapping
                    )
                    fig_party.update_traces(textposition='inside', textinfo='percent+label')
                    fig_party.update_layout(margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig_party, use_container_width=True)
                else:
                    st.warning("No party distribution data available")
            
            with col4:
                # Trading Activity Timeline
                st.subheader("Trading Activity Timeline")
                if not trades_data.empty:
                    daily_trades = trades_data.groupby('transaction_date').size().reset_index(name='trades')
                    fig_timeline = px.line(
                        daily_trades,
                        x='transaction_date',
                        y='trades',
                        title="Daily Trading Activity",
                        height=300
                    )
                    fig_timeline.update_layout(
                        margin=dict(l=20, r=20, t=40, b=20),
                        xaxis_title="Date",
                        yaxis_title="Number of Trades"
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)
                else:
                    st.warning("No trading activity data available")

# Clean up
if hasattr(data, 'close'):
    data.close()