// General react imports.
import { useMemo, useState } from 'react';

// MRT imports.
import {
  MantineReactTable,
  useMantineReactTable,
  type MRT_ColumnDef,
  type MRT_ColumnFilterFnsState,
  type MRT_ColumnFiltersState,
  type MRT_PaginationState,
  type MRT_SortingState,
} from 'mantine-react-table';

// UI elements.
import { ActionIcon, Tooltip, Box, Text, Stack } from '@mantine/core';
import { IconRefresh } from '@tabler/icons-react';

// react-query imports.
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from '@tanstack/react-query';

import './App.css'

// Conjunction datatype.
type Conjunction = {
  norad_ids: string;
  object_names: string;
  tca: string;
  dca: number;
  relative_speed: number;
  tca_diff: number;
  dca_diff: number;
  relative_speed_diff: number;
};

// Type expected from the conjunctions endpoint in the backend.
type ConjunctionApiResponse = {
  // The set of conjunctions to be displayed in the current page.
  rows: Array<Conjunction>,
  // The total number of conjunctions in the dataframe.
  tot_nrows: number;
}

// Bundle of parameters to be passed to useGetConjunctions(). This contains
// several MRT types describing aspects of the table state such as the sorting
// criteria, pagination, filtering, etc.
type useGetConjunctionsParams = {
  columnFilterFns: MRT_ColumnFilterFnsState;
  columnFilters: MRT_ColumnFiltersState;
  sorting: MRT_SortingState;
  pagination: MRT_PaginationState;
}

// react-query hook to fetch the list of conjunctions from the backend.
const useGetConjunctions = ({ columnFilterFns, columnFilters, sorting, pagination }: useGetConjunctionsParams) => {
  // API url.
  const url = 'http://localhost:8000/public_conjunctions/'

  // The body for the POST request. Here we are setting all the parameters
  // to be passed to the backend.
  const body = {
    begin: pagination.pageIndex * pagination.pageSize,
    nrows: pagination.pageSize,
    sorting: sorting,
    conjunctions_filter_fns: columnFilterFns,
    conjunctions_filters: columnFilters,
  };

  // Define the function that performs the API call.
  const queryFunction = async () => {
    // Options for the request.
    const requestOptions = {
      method: 'POST',
      // NOTE: it is important to set the correct content type here.
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    };

    // Make the request.
    const response = await fetch(url, requestOptions);

    // Fetch the result
    const responseData = await response.json();

    // Handle errors.
    if (!response.ok) {
      // Get error message from body or default to response status.
      const error = (responseData && responseData.message) || response.status;
      return Promise.reject(error);
    }

    return responseData;
  };

  // NOTE: what this does is essentially adding a few features on top of just
  // calling queryFunction() directly. The important bit for us it the caching behaviour:
  // if the result of a previous invocation of queryFunction() for a given 'body' was already
  // computed, the cached result will be returned.
  return useQuery<ConjunctionApiResponse>({
    // Here's the cache: we need to give a unique name ('conjunctions')
    // and pass the current 'body'.
    queryKey: ['conjunctions', body],
    queryFn: queryFunction,
    // NOTE: see https://github.com/TanStack/query/discussions/6460.
    placeholderData: (prev) => prev,
    // NOTE: do not refetch previously viewed pages until cache is more than 30 seconds old.
    staleTime: 30_000,
  });
};

// Function to create the table of conjunctions.
const ConjunctionsTable = () => {
  // Allowed predicates for range-based filters.
  const range_filter_modes = ['greaterThan', 'lessThan', 'between', 'betweenInclusive'];

  // Definition of the columns.
  const columns = useMemo<MRT_ColumnDef<Conjunction>[]>(
    () => [
      {
        accessorKey: 'norad_ids',
        header: 'Norad IDs',
        columnFilterModeOptions: ['contains'],
        enableSorting: false,
        Cell: ({ cell }) => {
          const [first, second] = cell.getValue<string>().split(" | ");

          return <Stack spacing="xs"><Box sx={(theme) => ({
            backgroundColor:
              theme.colors.blue[5],
            borderRadius: '5px',
            color: '#fff',
            textAlign: "center",
            padding: '2px',
          })}>
            <Text size="s" weight={700}>{first}</Text>
          </Box>
            <Box sx={(theme) => ({
              backgroundColor:
                theme.colors.blue[9],
              borderRadius: '5px',
              color: '#fff',
              textAlign: "center",
              padding: '2px',
            })}>
              <Text size="s" weight={700}>{second}</Text>
            </Box></Stack>
        },
      },
      {
        accessorKey: 'object_names',
        header: 'Names',
        columnFilterModeOptions: ['contains'],
        enableSorting: false,
        Cell: ({ cell }) => {
          const [first, second] = cell.getValue<string>().split(" | ");

          return <Stack spacing="xs"><Box sx={(theme) => ({
            backgroundColor:
              theme.colors.cyan[5],
            borderRadius: '5px',
            color: '#fff',
            textAlign: "center",
            padding: '2px',
          })}>
            <Text size="s" weight={700}>{first}</Text>
          </Box>
            <Box sx={(theme) => ({
              backgroundColor:
                theme.colors.cyan[9],
              borderRadius: '5px',
              color: '#fff',
              textAlign: "center",
              padding: '2px',
            })}>
              <Text size="s" weight={700}>{second}</Text>
            </Box></Stack>
        },
      },
      {
        accessorKey: 'tca',
        header: 'TCA (UTC)',
        enableColumnFilter: false
      },
      {
        accessorKey: 'dca',
        header: 'DCA (km)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
      {
        accessorKey: 'relative_speed',
        header: 'Rel. speed (km/s)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
      {
        accessorKey: 'tca_diff',
        header: 'TCA diff. (ms)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
      {
        accessorKey: 'dca_diff',
        header: 'DCA diff. (m)',
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
        columnFilterModeOptions: range_filter_modes,
      },
      {
        accessorKey: 'relative_speed_diff',
        header: 'Rel. speed diff. (m/s)',
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
        columnFilterModeOptions: range_filter_modes,
      },
    ],
    [],
  );

  // Manage MRT state that we want to pass to our API.
  const [columnFilters, setColumnFilters] = useState<MRT_ColumnFiltersState>(
    [],
  );
  // Filter modes. We have different defaults depending on the column.
  const [columnFilterFns, setColumnFilterFns] =
    useState<MRT_ColumnFilterFnsState>(
      {
        'norad_ids': 'contains', 'object_names': 'contains',
        'dca': 'betweenInclusive', 'relative_speed': 'betweenInclusive',
        'tca_diff': 'betweenInclusive', 'dca_diff': 'betweenInclusive',
        'relative_speed_diff': 'betweenInclusive',
      }
    );
  const [sorting, setSorting] = useState<MRT_SortingState>([]);
  const [pagination, setPagination] = useState<MRT_PaginationState>({
    pageIndex: 0,
    pageSize: 10,
  });

  // Call our custom react-query hook to fetch the data from the backend.
  const { data, isError, isFetching, isLoading, refetch } = useGetConjunctions({
    columnFilterFns,
    columnFilters,
    pagination,
    sorting,
  });

  // Fetch the conjunctions for the current page and the total
  // number of conjunctions from the response.
  const fetchedConjunctions = data?.rows ?? [];
  const totalRowCount = data?.tot_nrows ?? 0;

  const table = useMantineReactTable({
    columns,
    data: fetchedConjunctions,
    enableColumnFilterModes: true,
    columnFilterModeOptions: [],
    initialState: { density: 'xs' },
    manualFiltering: true,
    manualPagination: true,
    manualSorting: true,
    mantineToolbarAlertBannerProps: isError
      ? {
        color: 'red',
        children: 'Error loading data',
      }
      : undefined,
    onColumnFilterFnsChange: setColumnFilterFns,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    onSortingChange: setSorting,
    renderTopToolbarCustomActions: () => (
      <Tooltip label="Refresh Data">
        <ActionIcon onClick={() => refetch()}>
          <IconRefresh />
        </ActionIcon>
      </Tooltip>
    ),
    rowCount: totalRowCount,
    state: {
      columnFilterFns,
      columnFilters,
      isLoading,
      pagination,
      showAlertBanner: isError,
      showProgressBars: isFetching,
      sorting,
    },
    enableColumnDragging: true,
    enableColumnOrdering: true,
    enableGlobalFilter: false
  });

  return <MantineReactTable table={table} />;
};

const queryClient = new QueryClient();

const ConjunctionsQueryProvider = () => (
  <QueryClientProvider client={queryClient}>
    <ConjunctionsTable />
  </QueryClientProvider>
);

export default ConjunctionsQueryProvider;
